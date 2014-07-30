/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht

import java.net.InetSocketAddress

import akka.actor._
import akka.io.{IO, Udp}
import akka.util.{ByteString, ByteStringBuilder}
import org.abovobo.conversions.Bencode
import org.abovobo.dht
import org.abovobo.dht.message.{Message, Query, Response}
import org.abovobo.integer.Integer160

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
 * This actor is responsible for sending Kademlia UDP messages and receiving them.
 * It also manages Kademlia queries: when some sender initiates a query, this actor
 * will keep record about that fact and if remote party failed to respond in timely
 * manner, this actor will produce [[Agent.Failed]] command.
 *
 * In order to properly manage network connection(s), this actor must be properly managed
 * by its supervisor.
 *
 * @param endpoint   An endpoint at which this agent must listen.
 * @param timeout    A period in time during which the remote party must respond to a query.
 * @param retry      Time interval between bind attempts.
 * @param controller Reference to [[org.abovobo.dht.controller.Controller]] actor.
 */
class Agent(val endpoint: InetSocketAddress,
            val timeout: FiniteDuration,
            val retry: FiniteDuration,
            val controller: ActorRef)
  extends Actor with ActorLogging {

  import context.{dispatcher, system}

  /** @inheritdoc */
  override def preStart() = {
    this.log.debug("Agent#preStart (sending `Start` message)")
    //self ! Agent.Start()
    IO(Udp) ! Udp.Bind(self, this.endpoint)
  }

  /** @inheritdoc */
  override def postRestart(reason: Throwable) = {
    this.log.debug("Agent#postRestart (scheduling `Start` message)")
    //system.scheduler.scheduleOnce(this.retry, self, Agent.Start())
    system.scheduler.scheduleOnce(this.retry, IO(Udp), Udp.Bind(self, this.endpoint))
  }

  /** @inheritdoc */
  override def postStop() = {
    this.log.debug("Agent#postStop (unbinding socket and cancelling queries)")
    this.unbind()
    this.queries foreach { _._2._2.cancel() }
  }

  /** @inheritdoc */
  override def preRestart(reason: Throwable, message: Option[Any]) = {
    this.log.debug("Agent#preRestart (unbinding socket)")
    this.unbind()
  }

  /**
   * @inheritdoc
   *
   * Actually calls `become(this.ready)` as soon as [[akka.io.Udp.Bound]] message received.
   */
  override def receive = {

    case Udp.Bound(local) =>
      this.log.debug("Bound with local address {} socket is {}", local, this.sender())
      // remember socket reference
      this.socket = Some(this.sender())
      // sign a death pact
      this.context.watch(this.sender())
      // notify controller that agent is ready to go
      this.controller ! Agent.Bound(local)

    case Udp.Unbound =>
      this.log.debug("Agent unbound")

    case Udp.CommandFailed(cmd) => cmd match {
      case Udp.Bind(h, l, o) =>
        this.log.error("Failed to bind {} at {}. Crashing...", h, l)
        throw new Agent.FailedToBindException()

    }

    case Udp.Received(data, remote) => try {
      // parse received ByteString into a message instance
      val message = Message.parse(data) { tid: TID => this.queries.get(tid).map { _._1 } }

      // check if message completes pending transaction
      message match {
        case response: Response =>
          this.log.debug("Completing transaction " + response.tid + " by means of received response")
          this.queries.remove(response.tid).foreach { _._2.cancel() }
        case _ => // do nothing
      }
      // forward received message to controller
      this.controller ! Agent.Received(message, remote)
    } catch {
      case e: Message.ParsingException =>
        this.self ! Agent.Send(e.error, remote)
    }

    // `Send` command received
    case Agent.Send(message, remote) =>
      // if we are sending query - set up transaction monitor
      message match {
        case query: Query =>
          this.log.debug("Starting transaction " + query.tid)
          this.queries.put(
            query.tid,
            query -> system.scheduler.scheduleOnce(this.timeout, this.self, Agent.Timeout(query, remote)))
        case _ => // do nothing
      }
      // send serialized message to remote address
      this.socket.foreach(_ ! Udp.Send(Message.serialize(message), remote))

    // `Timeout` event occurred
    case Agent.Timeout(q, r) =>
      this.log.debug("Completing transaction " + q.tid + " by means of failure (timeout) for " + r)
      this.queries.remove(q.tid).foreach { _._2.cancel() }
      this.controller ! Agent.Failed(q)

    // To debug crashes
    case t: Throwable => throw t
  }

  /**
   * Stops watching the socket (actually "drops death contract") and sends
   * [[Udp.Unbind]] command to it.
   */
  private def unbind() = this.socket match {
    case Some(s) =>
      this.context.unwatch(s)
      s ! Udp.Unbind
      this.socket = None
    case None =>
      this.log.error("Unbinding <None> socket")
  }

  /// Instantiates a map of associations between transaction identifiers
  /// and cancellable tasks which will produce failure command if remote peer
  /// failed to respond in timely manner.
  private val queries = new mutable.HashMap[TID, (Query, Cancellable)]

  /// Optional reference to bound socket actor
  private var socket: Option[ActorRef] = None
}

/** Accompanying object */
object Agent {

  /**
   * Factory which creates [[Agent]] [[akka.actor.Props]] instance.
   *
   * @param endpoint   An adress/port to bind to to receive incoming packets
   * @param timeout    A period of time to wait for response from queried remote peer.
   * @param retry      Time interval between bind attempts.
   * @param controller Reference to [[org.abovobo.dht.controller.Controller]] actor.
   * @return           Properly configured [[akka.actor.Props]] instance.
   */
  def props(endpoint: InetSocketAddress, timeout: FiniteDuration, retry: FiniteDuration, controller: ActorRef): Props =
    Props(classOf[Agent], endpoint, timeout, retry, controller)

  /**
   * Base trait for all commands natively supported by [[Agent]] actor.
   */
  sealed trait Command

  /**
   * Command instructing actor to send given message to given remote peer.
   *
   * @param message A message to send.
   * @param remote  An address (IP/Port) to send message to.
   */
  case class Send(message: Message, remote: InetSocketAddress) extends Command
  
  /**
   * Instructs agent to fail transaction with given query due to timeout.
   * Used by agent scheduler.
   * 
   * @param query a query in question
   */
  case class Timeout(query: Query, remote: InetSocketAddress) extends Command

  /**
   * Base trait for all events handled or initiated by this actor
   */
  sealed trait Event

  /**
   * Indicates that this agent has successfully bound to particular UDP socket.
   *
   * @param local An address actor has bound to.
   */
  case class Bound(local: InetSocketAddress) extends Event

  /**
   * This event indicates that there was a query sent to remote peer, but remote peer failed to respond
   * in timely manner.
   *
   * @param query A query which has been sent initially.
   */
  case class Failed(query: Query) extends Event

  /**
   * This event indicates that there was a message received from the remote peer.
   *
   * @param message A message received.
   * @param remote  An address of the remote peer which sent us that message.
   */
  case class Received(message: Message, remote: InetSocketAddress) extends Event

  /**
   * This class is thrown by main event loop if there was an attempt to start already started Agent.
   */
  class InvalidStateException(val message: String) extends Exception(message)

  /**
   * This exception is thrown by main event loop if Bind attempt has failed.
   */
  class FailedToBindException extends Exception
}

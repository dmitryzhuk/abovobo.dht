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
import org.abovobo.dht.message.{Message, Query, Response, Error}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

/**
 * This actor represents network agent which uses UDP listener to receive packets
 * from remote nodes and to send packets to remote nodes. It's main functionality
 * is to translate [[Message]] instances into sequences of bencoded bytes suitable
 * for transmitting over network. It also controls request/reply pairs: if this
 * actor is requested to send [[Query]] message, it will wait for and recognize
 * corresponding [[Response]] message received from the remote peer. Note that
 * received [[Response]] message will be sent to the same actor which initiated
 * original [[Query]], while incoming [[Query]] messages receieved from remote
 * peers will be sent to registered handler actor.
 *
 * @param endpoint  An endpoint at which this agent must listen.
 * @param timeout   A period in time during which the remote party must respond to a query.
 * @param retry     Time interval between bind attempts.
 * @param handler   Reference to actor which will receive incoming [[Query]] messages.
 */
class Agent(val endpoint: InetSocketAddress,
            val timeout: FiniteDuration,
            val retry: FiniteDuration,
            val handler: ActorRef)
  extends Actor with ActorLogging {

  import context.{dispatcher, system}

  /** @inheritdoc */
  override def preStart() = {
    this.log.debug("Agent#preStart (sending `Start` message)")
    IO(Udp) ! Udp.Bind(self, this.endpoint)
  }

  /** @inheritdoc */
  override def postRestart(reason: Throwable) = {
    this.log.debug("Agent#postRestart (scheduling `Start` message)")
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
      // notify controller that Agent is ready to go
      this.handler ! Agent.Bound

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
        case r @ (_: Response | _: Error) =>
          this.log.debug("Completing transaction " + r.tid + " by means of received response")
          this.queries.remove(r.tid).foreach { transaction =>
            transaction._3 ! Agent.Received(message, remote)
            transaction._2.cancel()
          }
        case q: Query =>
          // forward received message to handler
          this.handler ! Agent.Received(message, remote)
      }
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
            (query, system.scheduler.scheduleOnce(this.timeout, this.self, Agent.Timeout(query, remote)), this.sender()))
        case _ => // do nothing
      }
      // send serialized message to remote address
      this.socket.foreach(_ ! Udp.Send(Message.serialize(message), remote))

    // `Timeout` event occurred
    case Agent.Timeout(q, r) =>
      this.log.debug("Completing transaction " + q.tid + " by means of failure (timeout) for " + r)
      this.queries.remove(q.tid).foreach { transaction =>
        transaction._3 ! Agent.Failed(q)
        transaction._2.cancel()
      }

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
  private val queries = new mutable.HashMap[TID, (Query, Cancellable, ActorRef)]

  /// Optional reference to bound socket actor
  private var socket: Option[ActorRef] = None
}

/** Accompanying object */
object Agent {

  /**
   * Factory which creates [[Agent]] [[akka.actor.Props]] instance.
   *
   * @param endpoint  An adress/port to bind to to receive incoming packets
   * @param timeout   A period of time to wait for response from queried remote peer.
   * @param retry     Time interval between bind attempts.
   * @param handler   Reference to [[org.abovobo.dht.controller.Controller]] actor.
   * @return          Properly configured [[akka.actor.Props]] instance.
   */
  def props(endpoint: InetSocketAddress, timeout: FiniteDuration, retry: FiniteDuration, handler: ActorRef): Props =
    Props(classOf[Agent], endpoint, timeout, retry, handler)

  /**
   * Factory which creates [[Agent]] [[Props]] instance with default `timeout` and `retry` durations.
   *
   * @param endpoint  An adress/port to bind to to receive incoming packets
   * @param handler   Reference to [[org.abovobo.dht.controller.Controller]] actor.
   * @return          Properly configured [[akka.actor.Props]] instance.
   */
  def props(endpoint: InetSocketAddress, handler: ActorRef): Props =
    this.props(endpoint, 5.seconds, 2.seconds, handler)

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
   * Fired when underlying UDP socket has properly been bound.
   */
  case object Bound extends Event

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

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

import org.abovobo.integer.Integer160
import java.net.InetSocketAddress
import akka.actor.{ActorLogging, Actor}
import scala.collection.mutable
import org.abovobo.dht.persistence.{Writer, Reader}
import scala.concurrent.duration.FiniteDuration

/**
 * This Actor actually controls processing of the messages implementing recursive DHT algorithms.
 *
 * @constructor     Creates new instance of controller with provided parameters.
 *
 * @param K         Max number of entries to send back to querier.
 * @param alpha     Number of concurrent lookup threads.
 * @param period    A period of rotating the token.
 * @param lifetime  A lifetime of the peer info.
 * @param reader    Instance of [[org.abovobo.dht.persistence.Reader]] used to access persisted data.
 * @param writer    Instance of [[org.abovobo.dht.persistence.Writer]] to update persisted DHT state.
 *
 * @author Dmitry Zhuk
 */
class Controller(val K: Int,
                 val alpha: Int,
                 val period: FiniteDuration,
                 val lifetime: FiniteDuration,
                 val reader: Reader,
                 val writer: Writer)
  extends Actor with ActorLogging {

  import Controller._

  /**
   * @inheritdoc
   *
   * Closes [[org.abovobo.dht.Responder]] instance.
   */
  override def postStop() = {
    this.responder.close()
  }

  /**
   * @inheritdoc
   *
   * Implements handling of [[org.abovobo.dht.Controller]]-specific events and commands.
   */
  override def receive = {
    // -- HANDLE COMMANDS
    // -- ---------------
    case Ping(address: InetSocketAddress) =>
      // Simply send Query.Ping to remote peer
      val query = new Query.Ping(this.factory.next(), this.reader.id().get)
      this.transactions.put(query.tid, query -> address)
      this.agent ! Agent.Send(query, address)

    case FindNode(target: Integer160) =>
      // --
      this.find(target)

    // -- HANDLE EVENTS
    // -- -------------
    case Failed(query) =>
      // when our query has failed just remove corresponding transaction
      // and properly notify routing table about this failure
      this.transactions.get(query.tid) match {
        case Some(pair) =>
          this.transactions.remove(query.tid)
          this.table ! Table.Failed(new Node(query.id, pair._2))
        case None => // Error: invalid transaction
          this.log.error("Failed event with invalid transaction: " + query)
      }

    case Received(message, remote) =>
      // received message handled differently depending on message type
      message match {
        case response: Response =>
          // if response has been received
          // close transaction, notify routing table and then
          // delegate execution to private `process` method
          this.transactions.get(response.tid) match {
            case Some(pair) =>
              this.transactions.remove(response.tid)
              this.table ! Table.Received(new Node(pair._1.id, pair._2), Message.Kind.Response)
              this.process(response, pair._1, pair._2)
            case None => // Error: invalid transaction
              this.log.error("Response message with invalid transaction: " + response)
          }

        case query: Query =>
          // if query has been received
          // notify routing table and then delegate execution to private `respond` method
          this.table ! Table.Received(new Node(query.id, remote), Message.Kind.Query)
          this.agent ! this.responder.respond(query, remote)

        case error: Error =>
          // if error message has been received
          // close transaction and notify routing table
          this.transactions.get(error.tid) match {
            case Some(pair) =>
              this.transactions.remove(error.tid)
              this.table ! Table.Received(new Node(pair._1.id, pair._2), Message.Kind.Error)
            case None => // Error: invalid transaction
              this.log.error("Error message with invalid transaction: " + error)
          }
      }
  }

  private def find(target: Integer160) = {
    //val nodes = this.klosest(target)
  }

  private def process(response: Response, query: Query, remote: InetSocketAddress) = {
    query match {
      case ping: Query.Ping =>
        // nothing to do
    }
  }

  /// Initializes sibling `agent` actor reference
  private lazy val agent = this.context.actorSelection("../agent")

  /// Initializes sibling `table` actor reference
  private lazy val table = this.context.actorSelection("../table")

  /// An instance of Responder to handle incoming queries
  private lazy val responder =
    new Responder(
      this.K,
      this.period,
      this.lifetime,
      this.agent,
      this.reader,
      this.writer,
      this.context.system.scheduler,
      this.context.dispatcher)

  /// Instantiate transaction ID factory
  private val factory = new TIDFactory

  /// Instance of tracked queries
  private val transactions = new mutable.HashMap[TID, (Query, InetSocketAddress)]
}

/** Accompanying object */
object Controller {

  /** Base trait for all events fired by other actors */
  sealed trait Event

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

  /** Base trait for all commands supported by this actor */
  sealed trait Command

  /**
   * This command instructs controller to send `ping` message to the given address.
   *
   * @param address An address to send `ping` message to.
   */
  case class Ping(address: InetSocketAddress) extends Command

  case class FindNode(target: Integer160) extends Command
}

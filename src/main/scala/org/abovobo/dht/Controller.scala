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
import akka.actor.{ActorRef, ActorLogging, Actor}
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
 * @param routers   A collection of addresses which can be used as inital seeds when own routing table is empty.
 * @param reader    Instance of [[org.abovobo.dht.persistence.Reader]] used to access persisted data.
 * @param writer    Instance of [[org.abovobo.dht.persistence.Writer]] to update persisted DHT state.
 *
 * @author Dmitry Zhuk
 */
class Controller(val K: Int,
                 val alpha: Int,
                 val period: FiniteDuration,
                 val lifetime: FiniteDuration,
                 val routers: Traversable[InetSocketAddress],
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
    case Ping(node: Node) =>
      // Simply send Query.Ping to remote peer
      val query = new Query.Ping(this.factory.next(), this.reader.id().get)
      this.transactions.put(query.tid, new Transaction(query, node, sender))
      this.agent ! Agent.Send(query, node.address)

    case FindNode(target: Integer160) =>
      val query = new Query.FindNode(this.factory.next(), this.reader.id().get, target)
      // -- this.transactions.put(query.tid, new Transaction(query, address, sender))
      // --
      this.find(target)

    case AnnouncePeer(node, token, infohash, port, implied) =>
      val query = new Query.AnnouncePeer(this.factory.next(), this.reader.id().get, infohash, port, token, implied)
      this.transactions.put(query.tid, new Transaction(query, node, sender))
      this.agent ! Agent.Send(query, node.address)

    // -- HANDLE EVENTS
    // -- -------------
    case Failed(query) =>
      // when our query has failed just remove corresponding transaction
      // and properly notify routing table about this failure
      this.transactions.remove(query.tid) match {
        case Some(transaction) =>
          this.table ! Table.Failed(transaction.remote)
        case None => // Error: invalid transaction
          this.log.error("Failed event with invalid transaction: " + query)
      }

    case Received(message, remote) =>
      // received message handled differently depending on message type
      message match {

        case query: Query =>
          // if query has been received
          // notify routing table and then delegate execution to `Responder`
          this.table ! Table.Received(new Node(query.id, remote), Message.Kind.Query)
          this.agent ! this.responder.respond(query, remote)

        case response: Response =>
          // if response has been received
          // close transaction, notify routing table and then
          // delegate execution to private `process` method
          this.transactions.remove(response.tid) match {
            case Some(transaction) =>
              this.table ! Table.Received(transaction.remote, Message.Kind.Response)
              // todo this.process(response, pair._1, pair._2)
            case None => // Error: invalid transaction
              this.log.error("Response message with invalid transaction: " + response)
          }

        case error: Error =>
          // if error message has been received
          // close transaction and notify routing table
          this.transactions.remove(error.tid) match {
            case Some(transaction) =>
              this.table ! Table.Received(transaction.remote, Message.Kind.Error)
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
      this.reader,
      this.writer,
      this.context.system.scheduler,
      this.context.dispatcher)

  /// Instantiate transaction ID factory
  private val factory = new TIDFactory

  /// Instance of tracked queries
  private val transactions = new mutable.HashMap[TID, Transaction]
}

/** Accompanying object */
object Controller {

  /**
   * This class defines transaction data.
   *
   * @param query     Original query which initiated transaction.
   * @param remote    Remote node.
   * @param requester An actor which has requested this transaction to begin.
   */
  class Transaction(val query: Query, val remote: Node, val requester: ActorRef)

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
   * @param node A node to send `ping` message to.
   */
  case class Ping(node: Node) extends Command

  /**
   * This command initiates concurrent recursive process of locating the node with given ID.
   * Note that this process may take significant amount of time and does not guarantee that
   * such node will be found even if it technically exists within the network.
   *
   * @param target 160-bit identifier of target node.
   */
  case class FindNode(target: Integer160) extends Command

  /**
   * This command initiates concurrent recursive process of locating peers which distribute
   * a content with given infohash. The process itself is identical to `find_node` but on some
   * step may provide actual peer info instead of node info.
   *
   * @param infohash 160-bit hash of the content to locate peers for.
   */
  case class GetPeers(infohash: Integer160) extends Command

  /**
   * This command instructs [[org.abovobo.dht.Controller]] to send [[org.abovobo.dht.Controller.AnnouncePeer]]
   * message to given address. Normally, this command is issued after [[org.abovobo.dht.Controller.GetPeers]]
   * command completed and produced a collection of nodes which can be used to announce itself as peer to them.
   *
   * @param node      A node to send message to.
   * @param token     Opaque token previously obtained from that remote peer.
   * @param infohash  An infohash of the content to announce itself as a peer at.
   * @param port      A port at which the peer is listening for content exchange (torrent).
   * @param implied   Flag indicating if port should be implied.
   */
  case class AnnouncePeer(node: Node,
                          token: Token,
                          infohash: Integer160,
                          port: Int,
                          implied: Boolean)
    extends Command
}

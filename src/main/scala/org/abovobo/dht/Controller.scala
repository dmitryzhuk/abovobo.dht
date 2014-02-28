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
import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import scala.collection.mutable
import org.abovobo.dht.persistence.{Writer, Reader}
import scala.concurrent.duration._

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
      if (!this.recursions.contains(target)) {
        this.recursions += target -> new Recursion(new Finder(target, this.K, this.routers), sender)
      }
      val recursion = this.recursions(target)
      val id = this.reader.id().get
      recursion.finder.take(this.alpha).foreach { node =>
        val query = new Query.FindNode(this.factory.next(), id, target)
        this.transactions.put(query.tid, new Transaction(query, node, sender))
        this.agent ! Agent.Send(query, node.address)
      }

    case GetPeers(infohash: Integer160) =>
      if (!this.recursions.contains(infohash)) {
        this.recursions += infohash -> new Recursion(new Finder(infohash, this.K, this.routers), sender)
      }
      val recursion = this.recursions(infohash)
      val id = this.reader.id().get
      recursion.finder.take(this.alpha).foreach { node =>
        val query = new Query.GetPeers(this.factory.next(), id, infohash)
        this.transactions.put(query.tid, new Transaction(query, node, sender))
        this.agent ! Agent.Send(query, node.address)
      }

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
          transaction.query match {
            case q: Query.FindNode =>
              this.recursions.get(q.target).foreach { r =>
                r.finder.fail(transaction.remote)
                this.iterate(r)
              }
            case q: Query.GetPeers =>
              this.recursions.get(q.infohash).foreach { r =>
                r.finder.fail(transaction.remote)
                this.iterate(r)
              }
            case _ => // do nothing for other queries
          }
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
              this.process(response, transaction)
            case None => // Error: invalid transaction
              this.log.error("Response message with invalid transaction: " + response)
          }

        case error: Error =>
          // if error message has been received
          // close transaction and notify routing table
          this.transactions.remove(error.tid) match {
            case Some(transaction) =>
              this.table ! Table.Received(transaction.remote, Message.Kind.Error)
              transaction.query match {
                case q: Query.FindNode =>
                  this.recursions.get(q.target).foreach { r =>
                    r.finder.fail(transaction.remote)
                    this.iterate(r)
                  }
                case q: Query.GetPeers =>
                  this.recursions.get(q.infohash).foreach { r =>
                    r.finder.fail(transaction.remote)
                    this.iterate(r)
                  }
                case _ => // do nothing for other queries
              }
            case None => // Error: invalid transaction
              this.log.error("Error message with invalid transaction: " + error)
          }
      }
  }

  /**
   * Processes given response for given transaction.
   *
   * @param response    A [[org.abovobo.dht.Response]] message from remote peer.
   * @param transaction A transaction instance that response was received for.
   */
  private def process(response: Response, transaction: Transaction) = {
    response match {
      case ping: Response.Ping =>
        transaction.requester ! Pinged()
      case fn: Response.FindNode =>
        this.recursions.get(transaction.query.asInstanceOf[Query.FindNode].target) match {
          case Some(r) =>
            r.finder.report(transaction.remote, fn.nodes, Nil, Array.empty)
            this.iterate(r)
          case None =>
            this.log.error("Failed to match recursion for " + transaction.query.asInstanceOf[Query.FindNode].target)
        }
      case gp: Response.GetPeersWithNodes =>
        this.recursions.get(transaction.query.asInstanceOf[Query.GetPeers].infohash) match {
          case Some(r) =>
            r.finder.report(transaction.remote, gp.nodes, Nil, gp.token)
            this.iterate(r)
          case None =>
            this.log.error("Failed to match recursion for " + transaction.query.asInstanceOf[Query.FindNode].target)
        }
      case gp: Response.GetPeersWithValues =>
        this.recursions.get(transaction.query.asInstanceOf[Query.GetPeers].infohash) match {
          case Some(r) =>
            r.finder.report(transaction.remote, Nil, gp.values, gp.token)
            this.iterate(r)
          case None =>
            this.log.error("Failed to match recursion for " + transaction.query.asInstanceOf[Query.FindNode].target)
        }
      case ap: Response.AnnouncePeer =>
        transaction.requester ! PeerAnnounced()
    }
  }

  /**
   * Checks current state of the [[org.abovobo.dht.Finder]] associated with given recursion
   * and makes the next move depending on the state value.
   *
   * @param r Recursion instance to interate.
   */
  private def iterate(r: Recursion) = {
    r.finder.state match {
      case Finder.State.Failed =>
        r.requester ! NotFound()
        this.recursions -= r.finder.target
      case Finder.State.Succeeded =>
        r.requester ! Found(r.finder.nodes, r.finder.peers, r.finder.tokens)
        this.recursions -= r.finder.target
      case Finder.State.Continue =>
        val id = this.reader.id().get
        r.finder.take(this.alpha).foreach { node =>
          val query = new Query.FindNode(this.factory.next(), id, r.finder.target)
          this.transactions.put(query.tid, new Transaction(query, node, r.requester))
          this.agent ! Agent.Send(query, node.address)
        }
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

  /// Collection of pending transactions
  private val transactions = new mutable.HashMap[TID, Transaction]

  /// Collection of pending recursive procedures
  private val recursions = new mutable.HashMap[Integer160, Recursion]
}

/** Accompanying object */
object Controller {

  /** Generates [[akka.actor.Props]] instance with supplied parameters */
  def props(K: Int,
            alpha: Int,
            period: FiniteDuration,
            lifetime: FiniteDuration,
            routers: Traversable[InetSocketAddress],
            reader: Reader,
            writer: Writer) =
    Props(classOf[Controller], K, alpha, period, lifetime, routers, reader, writer)

  /** Generates [[akka.actor.Props]] instance with most parameters set to their default values */
  def props(routers: Traversable[InetSocketAddress], reader: Reader, writer: Writer): Props =
    this.props(8, 3, 5.minutes, 30.minutes, routers, reader, writer)

  /**
   * This class defines transaction data.
   *
   * @param query     Original query which initiated transaction.
   * @param remote    Remote node.
   * @param requester An actor which has requested this transaction to begin.
   */
  class Transaction(val query: Query, val remote: Node, val requester: ActorRef)

  /**
   * This class defines data associated with `find_node` or `get_peers` recursive procedure.
   *
   * @param finder    An instance of [[org.abovobo.dht.Finder]] which collects data.
   * @param requester An original requester of this operation.
   */
  class Recursion(val finder: Finder, val requester: ActorRef)

  /** Base trait for all events handled or initiated by this actor */
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

  /** Indicates that node has been successfully pinged */
  case class Pinged() extends Event

  /** Indicates that peer has been successfully announced */
  case class PeerAnnounced() extends Event

  /**
   * Indicates that recursive `find_node` or `get_peers` operation has been successfully completed.
   *
   * @param nodes   Collected nodes (only closest maximum K nodes provided).
   * @param peers   Collected peers (only for `get_peers` operation).
   * @param tokens  Collection of node id -> token associations (only for `get_peers` operation).
   */
  case class Found(nodes: Traversable[Node], peers: Traversable[Peer], tokens: Map[Integer160, Token]) extends Event

  /** Indicates that recursive `find_node` or `get_peers` operation has failed. */
  case class NotFound() extends Event

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

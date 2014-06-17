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
import org.abovobo.dht.Table.Updated
import org.abovobo.dht.Table.Inserted
import scala.concurrent.Await

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
  
  val system = context.system
  import system.dispatcher
  import Controller._
  
  override def preStart() = {}
  
  override def postRestart(reason: Throwable): Unit = {
    log.warning("Restarting controller due to " + reason)
    // not calling preStart and sending messages again, facility makes actor restarting transparent
    //preStart()
  }  

  /**
   * @inheritdoc
   *
   * Implements handling of [[org.abovobo.dht.Controller]]-specific events and commands.
   */
  override def receive = {
    // To avoid "unhandled message" logging. TODO: do we need to process these messages?        
    case Table.Updated =>
    case Table.Inserted(_) =>
    case Table.Split(_, _) =>
   

    // -- HANDLE COMMANDS
    // -- ---------------
    case Ping(node: Node) =>
      // Simply send Query.Ping to remote peer
      val query = new Query.Ping(this.factory.next(), this.reader.id().get)
      this.transactions.put(query.tid, new Transaction(query, node, this.sender()))
      this.agent ! Agent.Send(query, node.address)

    case FindNode(target: Integer160) =>
      if (!this.recursions.contains(target)) {
        this.recursions += target -> new NodesFinder(this.sender(), target, this.K, klosest(this.alpha, target))
      }
      val finder = this.recursions(target)
      val id = this.reader.id().get
      finder.take(this.alpha).foreach { node =>
        val query = new Query.FindNode(this.factory.next(), id, target)
        this.transactions.put(query.tid, new Transaction(query, node, this.sender()))
        this.agent ! Agent.Send(query, node.address)
      }

    case GetPeers(infohash: Integer160) =>
      if (!this.recursions.contains(infohash)) {
        this.recursions += infohash -> new PeersFinder(this.sender(), infohash, this.K, klosest(this.alpha, infohash))
      }
      val finder = this.recursions(infohash)
      val id = this.reader.id().get
      finder.take(this.alpha).foreach { node =>
        val query = new Query.GetPeers(this.factory.next(), id, infohash)
        this.transactions.put(query.tid, new Transaction(query, node, this.sender()))
        this.agent ! Agent.Send(query, node.address)
      }

    case AnnouncePeer(node, token, infohash, port, implied) =>
      val query = new Query.AnnouncePeer(this.factory.next(), this.reader.id().get, infohash, port, token, implied)
      this.transactions.put(query.tid, new Transaction(query, node, this.sender()))
      this.agent ! Agent.Send(query, node.address)
      
      
    case SendPluginMessage(message, node) =>
      // just forward message to agent for now.
      // maybe its possible to reuse plugins query->response style traffic for DHT state update
      // then, tid field and timeouts should be managed by this DHT Controller and Agent

      this.agent ! Agent.Send(message, node.address)
      
    case PutPlugin(pid, plugin) => 
      this.plugins.put(pid.number, plugin)
    
    case RemovePlugin(pid) => 
      this.plugins.remove(pid.number)
      
    case RotateTokens =>
      system.scheduler.scheduleOnce(this.period, self, RotateTokens)
      responder.rotateTokens()

    case CleanupPeers =>
      system.scheduler.scheduleOnce(this.lifetime, self, CleanupPeers)
      responder.cleanupPeers()      

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
              this.recursions.get(q.target).foreach { finder =>
                finder.fail(transaction.remote)
                finder.iterate()
              }
            case q: Query.GetPeers =>
              this.recursions.get(q.infohash).foreach { finder =>
                finder.fail(transaction.remote)
                finder.iterate()
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
          val node = new Node(query.id, remote)
          this.table ! Table.Received(node, Message.Kind.Query)
          this.agent ! this.responder.respond(query, node)

        case response: Response =>
          // if response has been received
          // close transaction, notify routing table and then
          // delegate execution to private `process` method
          this.transactions.remove(response.tid) match {
            case Some(transaction) =>
              // don't notify table about routers activity
              if (transaction.remote.id != Integer160.zero) {
                /// XXX: handle case when node id is changed i.e. transaction.remote.id != response.id
                this.table ! Table.Received(transaction.remote, Message.Kind.Response)
              } 
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
                  this.recursions.get(q.target).foreach { finder =>
                    finder.fail(transaction.remote)
                    finder.iterate()
                  }
                case q: Query.GetPeers =>
                  this.recursions.get(q.infohash).foreach { finder =>
                    finder.fail(transaction.remote)
                    finder.iterate()
                  }
                case _ => // do nothing for other queries
              }
            case None => // Error: invalid transaction
              this.log.error("Error message with invalid transaction: " + error)
          }
        case pm: PluginMessage =>
          this.plugins.get(pm.pluginId.number) match {
            case Some(plugin) => plugin ! Received(pm, remote)
            case None => {
              this.log.error("Error, message to non-existing plugin.")
              this.agent ! Agent.Send(new Error(pm.tid, Error.ERROR_CODE_UNKNOWN, "No such plugin"), remote)
            }
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
          case Some(finder) =>
            finder.report(transaction.remote, fn.nodes, Nil, Array.empty)
            finder.iterate()
          case None =>
            this.log.error("Failed to match recursion for " + transaction.query.asInstanceOf[Query.FindNode].target)
        }
      case gp: Response.GetPeersWithNodes =>
        this.recursions.get(transaction.query.asInstanceOf[Query.GetPeers].infohash) match {
          case Some(finder) =>
            finder.report(transaction.remote, gp.nodes, Nil, gp.token)
            finder.iterate()
          case None =>
            this.log.error("Failed to match recursion for " + transaction.query.asInstanceOf[Query.FindNode].target)
        }
      case gp: Response.GetPeersWithValues =>
        this.recursions.get(transaction.query.asInstanceOf[Query.GetPeers].infohash) match {
          case Some(finder) =>
            finder.report(transaction.remote, Nil, gp.values, gp.token)
            finder.iterate()
          case None =>
            this.log.error("Failed to match recursion for " + transaction.query.asInstanceOf[Query.FindNode].target)
        }
      case ap: Response.AnnouncePeer =>
        transaction.requester ! PeerAnnounced()
    }
  }
  
  private def klosest(n: Int, target: Integer160) = {
    val fromTable = this.reader.klosest(n, target)
    if (fromTable.size < n) {
      // routers are always last
      fromTable ++ routersNodes
    } else { 
      fromTable
    }
  }

  /// Initializes sibling `agent` actor reference
  private lazy val agent = Await.result(this.context.actorSelection("../agent").resolveOne(5 seconds), 6 seconds)

  /// Initializes sibling `table` actor reference
  private lazy val table = Await.result(this.context.actorSelection("../table").resolveOne(5 seconds), 6 seconds)
  
  /// An instance of Responder to handle incoming queries
  private lazy val responder =
    new Responder(
      this.K,
      this.period,
      this.lifetime,
      this.reader,
      this.writer)
  
  self ! RotateTokens
  self ! CleanupPeers

  /// Instantiate transaction ID factory
  private val factory = TIDFactory.random

  /// Collection of pending transactions
  private val transactions = new mutable.HashMap[TID, Transaction]

  /// Collection of pending recursive procedures
  private val recursions = new mutable.HashMap[Integer160, Finder]
  
  /// Routers nodes marked with zero IDs to avoid adding them to routing tables
  private val routersNodes = routers.map { ra => new Node(Integer160.zero, ra) }
  
  /// Active plugins
  private val plugins = new mutable.HashMap[Long, ActorRef]
  
  private abstract class AbstractFinder(val requester: ActorRef, target: Integer160, K: Int, seeds: Traversable[Node]) extends Finder(target, K, seeds) {
    /**
     * Checks current state of the [[org.abovobo.dht.Finder]] associated with given recursion
     * and makes the next move depending on the state value.
     */
    def iterate() = this.state match {
        case Finder.State.Failed =>
          requester ! NotFound()
          Controller.this.recursions -= this.target
        case Finder.State.Succeeded =>
          requester ! Found(this.nodes, this.peers, this.tokens)
          Controller.this.recursions -= this.target
        case Finder.State.Continue =>
          val id = Controller.this.reader.id().get
          this.take(Controller.this.alpha).foreach { node =>
            val query = newQuery(Controller.this.factory.next(), id)
            Controller.this.transactions.put(query.tid, new Transaction(query, node, requester))
            Controller.this.agent ! Agent.Send(query, node.address)
          }
      }

    def newQuery(tid: TID, id: Integer160): Query
  }
  
  private class PeersFinder(requester: ActorRef, target: Integer160, K: Int, seeds: Traversable[Node]) extends AbstractFinder(requester, target, K, seeds) {
    def newQuery(tid: TID, id: Integer160) = new Query.GetPeers(tid, id, this.target)
  }
  
  private class NodesFinder(requester: ActorRef, target: Integer160, K: Int, seeds: Traversable[Node]) extends AbstractFinder(requester, target, K, seeds) {
    def newQuery(tid: TID, id: Integer160) = new Query.FindNode(tid, id, this.target)
  }
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
  case class Found(nodes: Traversable[Node], peers: Traversable[Peer], tokens: scala.collection.Map[Integer160, Token]) extends Event

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
    
  case class SendPluginMessage(message: PluginMessage, node: Node) extends Command
    
  case class PutPlugin(pid: Plugin.PID, plugin: ActorRef) extends Command
  case class RemovePlugin(pid: Plugin.PID) extends Command
  
  case object RotateTokens extends Command
  case object CleanupPeers extends Command
}
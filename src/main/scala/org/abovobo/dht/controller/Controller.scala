/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.controller

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.abovobo.dht
import org.abovobo.dht._
import org.abovobo.dht.message.{Message, Query, Response}
import org.abovobo.dht.persistence.{Reader, Writer}
import org.abovobo.integer.Integer160

import scala.collection.mutable
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
 * @param timeout   @see Actor.timeout
 * @param routers   A collection of addresses which can be used as inital seeds when own routing table is empty.
 * @param reader    Instance of [[org.abovobo.dht.persistence.Reader]] used to access persisted data.
 * @param writer    Instance of [[org.abovobo.dht.persistence.Writer]] to update persisted DHT state.
 * @param agent     Reference to an Agent actor.
 * @param table     Reference to a Table actor.
 *
 * @author Dmitry Zhuk
 */
class Controller(val K: Int,
                 val alpha: Int,
                 val period: FiniteDuration,
                 val lifetime: FiniteDuration,
                 val timeout: FiniteDuration,
                 val routers: Traversable[InetSocketAddress],
                 val reader: Reader,
                 val writer: Writer,
                 val agent: ActorRef,
                 val table: ActorRef)
  extends Actor with ActorLogging {
  
  import this.context.dispatcher
  import Controller._

  override def preStart() = {}
  
  override def postRestart(reason: Throwable): Unit = {
    log.warning("Restarting controller due to " + reason)
    // not calling preStart and sending messages again, facility makes actor restarting transparent
  }
  
  override def postStop() {
    this.timerTasks.foreach(_.cancel())
  }

  /**
   * @inheritdoc
   *
   * Implements handling of [[controller.Controller]]-specific events and commands.
   */
  override def receive = {

    // To avoid "unhandled message" logging
    case r: Table.Result =>   

    // -- HANDLE COMMANDS
    // -- ---------------
    case Ping(node: Node) =>
      // Simply send Query.Ping to remote peer
      val query = new Query.Ping(this.factory.next(), this.reader.id().get)
      this.transactions.put(query.tid, new Transaction(query, node, this.sender()))
      this.agent ! Agent.Send(query, node.address)

    case AnnouncePeer(node, token, infohash, port, implied) =>
      // Simply send Query.AnnouncePeer message to remote peer
      val query = new Query.AnnouncePeer(this.factory.next(), this.reader.id().get, infohash, port, token, implied)
      this.transactions.put(query.tid, new Transaction(query, node, this.sender()))
      this.agent ! Agent.Send(query, node.address)

    case FindNode(target: Integer160) => this.iterate(target, this.sender()) { () =>
      new Query.FindNode(this.factory.next(), this.reader.id().get, target)
    }

    case GetPeers(infohash: Integer160) => this.iterate(infohash, this.sender()) { () =>
      new Query.GetPeers(this.factory.next(), this.reader.id().get, infohash)
    }

    case RotateTokens => responder.rotateTokens()
    case CleanupPeers => responder.cleanupPeers()

    // -- HANDLE EVENTS
    // -- -------------
    // when our query has failed just remove corresponding transaction
    // and properly notify routing table about this failure
    case Failed(query) => this.transactions.remove(query.tid).foreach(this.fail)

    case Received(message, remote) =>
      // received message handled differently depending on message type
      message match {

        // if query has been received
        // notify routing table and then delegate execution to `Responder`
        case query: Query =>
          val node = new Node(query.id, remote)
          this.table ! Table.Received(node, Message.Kind.Query)
          this.agent ! this.responder.respond(query, node)

        // if response has been received
        // close transaction, notify routing table and then
        // delegate execution to private `process` method
        case response: Response => this.transactions.remove(response.tid).foreach(this.process(response, _))

        // if error message has been received
        // close transaction and notify routing table
        case error: dht.message.Error => this.transactions.remove(error.tid).foreach(this.fail)

        /// XXX To be removed
        case pm: dht.message.Plugin =>
          this.plugins.get(pm.pid.value) match {
            case Some(plugin) => plugin ! Received(pm, remote)
            case None =>
              this.log.error("Error, message to non-existing plugin.")
              this.agent ! Agent.Send(
                new dht.message.Error(pm.tid, dht.message.Error.ERROR_CODE_UNKNOWN, "No such plugin"), remote)
          }
      }

    // just forward message to agent for now.
    // maybe its possible to reuse plugins query->response style traffic for DHT state update
    // then, tid field and timeouts should be managed by this DHT Controller and Agent
    /// XXX To be removed
    case SendPluginMessage(message, node) => this.agent ! Agent.Send(message, node.address)
    case PutPlugin(pid, plugin) =>  this.plugins.put(pid.value, plugin)
    case RemovePlugin(pid) =>  this.plugins.remove(pid.value)
  }

  /**
   * Grabs target 160-bit ID for recursions or None if other query.
   *
   * @param q An instance of query to get target id from.
   * @return Some id or None.
   */
  private def target(q: Query): Option[(Integer160, (TID, Integer160, Integer160) => Query)] = q match {
    case fn: Query.FindNode =>
      Some((fn.target, (tid: TID, id: Integer160, target: Integer160) => new Query.FindNode(tid, id, target)))
    case gp: Query.GetPeers =>
      Some((gp.infohash, (tid: TID, id: Integer160, infohash: Integer160) => new Query.GetPeers(tid, id, infohash)))
    case _ => None
  }

  /**
   * Handles failed transaction (by means of timeout or received Error message).
   * Notifies Table actor about failure and reports failure to Finder, if we are dealing with recursion.
   *
   * @param transaction Transaction instance to handle fail for.
   */
  private def fail(transaction: Transaction) = {
    // don't notify table about routers activity
    if (transaction.remote.id != Integer160.zero) {
      this.table ! Table.Received(transaction.remote, Message.Kind.Error)
    }
    // if we are dealing with recursion, get target id and
    // fail it in finder and finally trigger next iteration
    this.target(transaction.query).foreach { target =>
      this.recursions.get(target._1) foreach { finder =>
        finder.fail(transaction.remote)
        this.iterate(target._1, transaction.requester) { () =>
          target._2(this.factory.next(), this.reader.id().get, target._1)
        }
      }
    }
  }

  /**
   * Performs next iteration of recursive lookup procedure.
   *
   * @param id     An id of interest (node id for FIND_NODE RPC or infohash for FIND_VALUE RPC).
   * @param sender An original sender of the initial command which initiated recursion.
   *
   * @tparam T     Type of query to use (can be [[Query.FindNode]] or [[Query.GetPeers]])
   */
  private def iterate[T <: Query](id: Integer160, sender: ActorRef)(q: () => T) = {
    val f = this.recursions.get(id) match {
      case None =>
        val finder = new Finder(id, this.K, this.alpha, klosest(this.alpha, id))
        this.recursions.put(id, finder)
        finder
      case Some(finder) => finder
    }
    def round(n: Int) = f.take(n) foreach { node =>
      val query = q()
      this.transactions.put(query.tid, new Transaction(query, node, sender))
      this.agent ! Agent.Send(query, node.address)
    }
    this.log.debug("Finder state when #iterate: " + f.state)
    f.state match {
      case Finder.State.Wait => // do nothing
      case Finder.State.Continue => round(this.alpha)
      case Finder.State.Finalize => round(this.K)
      case Finder.State.Succeeded => sender ! Controller.Found(f.nodes, f.peers, f.tokens)
      case Finder.State.Failed => sender ! Controller.NotFound()
    }
  }

  /**
   * Processes given response for given transaction.
   *
   * @param response    A [[Response]] message from remote peer.
   * @param transaction A transaction instance that response was received for.
   */
  private def process(response: Response, transaction: Transaction) = {
    if (transaction.remote.id != Integer160.zero) {
      this.table ! Table.Received(transaction.remote, Message.Kind.Response)
    }
    response match {

      case ping: Response.Ping =>
        transaction.requester ! Pinged()

      case fn: Response.FindNode =>
        val target = transaction.query.asInstanceOf[Query.FindNode].target
        this.recursions.get(target).foreach { finder =>
          finder.report(transaction.remote, fn.nodes, Nil, Array.empty)
          this.iterate(target, transaction.requester) { () =>
            new Query.FindNode(this.factory.next(), this.reader.id().get, target)
          }
        }

      case gp: Response.GetPeersWithNodes =>
        val infohash = transaction.query.asInstanceOf[Query.GetPeers].infohash
        this.recursions.get(infohash).foreach { finder =>
          finder.report(transaction.remote, gp.nodes, Nil, gp.token)
          this.iterate(infohash, transaction.requester) { () =>
            new Query.GetPeers(this.factory.next(), this.reader.id().get, infohash)
          }
        }

      case gp: Response.GetPeersWithValues =>
        val infohash = transaction.query.asInstanceOf[Query.GetPeers].infohash
        this.recursions.get(infohash).foreach { finder =>
          finder.report(transaction.remote, Nil, gp.values, gp.token)
          this.iterate(infohash, transaction.requester) { () =>
            new Query.GetPeers(this.factory.next(), this.reader.id().get, infohash)
          }
        }

      case ap: Response.AnnouncePeer =>
        transaction.requester ! PeerAnnounced()
    }
  }

  /// Wraps Reader's `klosest` method adding routers to the output if the table has not enough entries
  private def klosest(n: Int, target: Integer160) = this.reader.klosest(n, target) match {
    case nn: Seq[Node] if nn.size < n => nn ++ (this.routers map { ra => new Node(Integer160.zero, ra) })
    case enough: Seq[Node] => enough
  }

  /// An instance of Responder to handle incoming queries
  private lazy val responder = new Responder(this.K, this.period, this.lifetime, this.reader, this.writer)
  
  val timerTasks = 
    this.context.system.scheduler.schedule(Duration.Zero, this.period, self, RotateTokens) ::
    this.context.system.scheduler.schedule(this.lifetime, this.lifetime, self, CleanupPeers) :: Nil

  /// Instantiate transaction ID factory
  private val factory = TIDFactory.random

  /// Collection of pending transactions
  private val transactions = new mutable.HashMap[TID, Transaction]

  /// Collection of pending recursive procedures
  private val recursions = new mutable.HashMap[Integer160, Finder]

  /// Active plugins
  /// XXX To be removed from Controller
  private val plugins = new mutable.HashMap[Long, ActorRef]
}

/** Accompanying object */
object Controller {

  /** Generates [[akka.actor.Props]] instance with supplied parameters */
  def props(K: Int,
            alpha: Int,
            period: FiniteDuration,
            lifetime: FiniteDuration,
            timeout: FiniteDuration,
            routers: Traversable[InetSocketAddress],
            reader: Reader,
            writer: Writer,
            agent: ActorRef,
            table: ActorRef) =
    Props(classOf[Controller], K, alpha, period, lifetime, timeout, routers, reader, writer, agent, table)

  /** Generates [[akka.actor.Props]] instance with most parameters set to their default values */
  def props(routers: Traversable[InetSocketAddress], reader: Reader, writer: Writer, agent: ActorRef, table: ActorRef): Props =
    this.props(8, 3, 5.minutes, 30.minutes, 10.seconds, routers, reader, writer, agent, table)

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
  case class Found(nodes: Traversable[Node],
                   peers: Traversable[Peer],
                   tokens: scala.collection.Map[Integer160, Token])
    extends Event

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
   * This command instructs [[controller.Controller]] to send [[controller.Controller.AnnouncePeer]]
   * message to given address. Normally, this command is issued after [[controller.Controller.GetPeers]]
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

  /// Base class for private commands which Controller sends to itself
  private trait SelfCommand extends Command

  /// Command to self, which instructs TokenManager to rotate tokens
  private case object RotateTokens extends SelfCommand

  /// Command to self, which causes old peers to be deleted form the storage
  private case object CleanupPeers extends SelfCommand


  /// XXX These plugin-related messages will be removed soon
  case class SendPluginMessage(message: org.abovobo.dht.message.Plugin, node: Node) extends Command
  case class PutPlugin(pid: PID, plugin: ActorRef) extends Command
  case class RemovePlugin(pid: PID) extends Command
}

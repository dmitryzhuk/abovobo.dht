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
 * @param K           System wide metric which is used to define size of the DHT table bucket.
 * @param alpha       Number of concurrent lookup threads in recursive FIND_NODE RPCs.
 * @param period      A period of rotating the token.
 * @param lifetime    A lifetime of the peer info.
 * @param routers     A collection of addresses which can be used as inital seeds when own routing table is empty.
 * @param reader      Instance of [[org.abovobo.dht.persistence.Reader]] used to access persisted data.
 * @param writer      Instance of [[org.abovobo.dht.persistence.Writer]] to update persisted DHT state.
 * @param table       Reference to [[Table]] actor.
 *
 * @author Dmitry Zhuk
 */
class Controller(val K: Int,
                 val alpha: Int,
                 val period: FiniteDuration,
                 val lifetime: FiniteDuration,
                 val routers: Traversable[InetSocketAddress],
                 val reader: Reader,
                 val writer: Writer,
                 val table: ActorRef)
  extends Actor with ActorLogging {

  import this.context.dispatcher

  /** @inheritdoc */
  override def preStart() = {
    this.log.debug("Controller#preStart")
    this.context.watch(this.table)
  }

  /** @inheritdoc */
  override def postStop() = {
    this.log.debug("Controller#postStop")
    this.context.unwatch(this.table)
    this.timerTasks.foreach(_.cancel())
  }

  /**
   * Defines initial event loop which only handles [[Agent.Bound]] event and immediately
   * switches to [[Controller.working()]] function.
   */
  def waiting: Receive = {
    case Agent.Bound =>
      this.context.become(this.working(this.sender()))
      this.table ! Controller.Ready
  }

  /**
   * General message handling function.
   *
   * @param agent Reference to Bound [[Agent]] actor.
   */
  def working(agent: ActorRef): Receive = {

    // To avoid "unhandled message" logging
    case r: Table.Result =>

    // -- HANDLE COMMANDS
    // -- ---------------
    case Controller.Ping(node: NodeInfo) =>
      // Simply send Query.Ping to remote peer
      val query = new Query.Ping(this.factory.next(), this.reader.id().get)
      this.transactions.put(query.tid, new Controller.Transaction(query, node, this.sender()))
      agent ! Agent.Send(query, node.address)

    case Controller.AnnouncePeer(node, token, infohash, port, implied) =>
      // Simply send Query.AnnouncePeer message to remote peer
      val query = new Query.AnnouncePeer(this.factory.next(), this.reader.id().get, infohash, port, token, implied)
      this.transactions.put(query.tid, new Controller.Transaction(query, node, this.sender()))
      agent ! Agent.Send(query, node.address)

    case Controller.FindNode(target: Integer160) => this.iterate(target, None, this.sender(), agent) { () =>
      new Query.FindNode(this.factory.next(), this.reader.id().get, target)
    }

    case Controller.GetPeers(infohash: Integer160) => this.iterate(infohash, None, this.sender(), agent) { () =>
      new Query.GetPeers(this.factory.next(), this.reader.id().get, infohash)
    }

    // Invokes tokens rotation procedure
    case Controller.RotateTokens => this.responder.rotateTokens()

    // Invokes peer cleanup procedure
    case Controller.CleanupPeers => this.responder.cleanupPeers()

    // -- HANDLE EVENTS
    // -- -------------
    // when our query has failed just remove corresponding transaction
    // and properly notify routing table about this failure
    case Agent.Failed(query, remote) =>
      this.transactions.remove(query.tid).foreach(this.fail(_, agent))

    case Agent.Received(message, remote) =>
      // received message handled differently depending on message type
      message match {

        // if query has been received
        // notify routing table and then delegate execution to `Responder`
        case query: Query =>
          val node = new NodeInfo(query.id, remote)
          this.table ! Table.Received(node, Message.Kind.Query)
          agent ! this.responder.respond(query, node)

        // if response has been received
        // close transaction, notify routing table and then
        // delegate execution to private `process` method
        case response: Response => this.transactions.remove(response.tid).foreach(this.process(response, _, agent))

        // if error message has been received
        // close transaction and notify routing table
        case error: dht.message.Error => this.transactions.remove(error.tid).foreach(this.fail(_, agent))

        /// XXX To be removed
        case pm: dht.message.Plugin =>
          this.plugins.get(pm.pid.value) match {
            case Some(plugin) => plugin ! Agent.Received(pm, remote)
            case None =>
              this.log.error("Error, message to non-existing plugin.")
              agent ! Agent.Send(
                new dht.message.Error(pm.tid, dht.message.Error.ERROR_CODE_UNKNOWN, "No such plugin"), remote)
          }
      }

    // just forward message to agent for now.
    // maybe its possible to reuse plugins query->response style traffic for DHT state update
    // then, tid field and timeouts should be managed by this DHT Controller and Agent
    /// XXX To be removed
    case Controller.SendPluginMessage(message, node) => agent ! Agent.Send(message, node.address)
    case Controller.PutPlugin(pid, plugin) =>  this.plugins.put(pid.value, plugin)
    case Controller.RemovePlugin(pid) =>  this.plugins.remove(pid.value)
  }

  /** @inheritdoc */
  override def receive = this.waiting

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
   * @param agent       A reference to [[Agent]] actor.
   */
  private def fail(transaction: Controller.Transaction, agent: ActorRef) = {
    this.log.debug("Failing transaction {}", transaction.query)
    // don't notify table about routers activity
    if (transaction.remote.id != Integer160.zero) {
      this.table ! Table.Received(transaction.remote, Message.Kind.Error)
    }
    // if we are dealing with recursion, get target id and
    // fail it in finder and finally trigger next iteration
    this.target(transaction.query).foreach { target =>
      this.recursions.get(target._1) foreach { finder =>
        finder.fail(transaction.remote)
        this.iterate(target._1, Some(finder), transaction.requester, agent) { () =>
          target._2(this.factory.next(), this.reader.id().get, target._1)
        }
      }
    }
  }

  /**
   * Performs next iteration of recursive lookup procedure.
   *
   * @param id     An id of interest (node id for FIND_NODE RPC or infohash for FIND_VALUE RPC).
   * @param finder Optional instance of finder associated with this recursion.
   * @param sender An original sender of the initial command which initiated recursion.
   * @param agent  A reference to [[Agent]] actor.
   *
   * @tparam T     Type of query to use (can be [[Query.FindNode]] or [[Query.GetPeers]])
   */
  private def iterate[T <: Query](id: Integer160, finder: Option[Finder], sender: ActorRef, agent: ActorRef)(q: () => T) = {
    val f = finder.getOrElse({
      val finder = new Finder(id, this.K, this.alpha, klosest(this.alpha, id))
      this.recursions.put(id, finder)
      finder
    })
    def round(n: Int) = f.take(n) foreach { node =>
      val query = q()
      this.transactions.put(query.tid, new Controller.Transaction(query, node, sender))
      agent ! Agent.Send(query, node.address)
    }
    this.log.debug("Finder state before #iterate: " + f.state)
    f.state match {
      case Finder.State.Wait => // do nothing
      case Finder.State.Continue => round(this.alpha)
      case Finder.State.Finalize => round(this.K)
      case Finder.State.Succeeded =>
        this.log.debug("Sending SUCCEEDED to " + sender)
        sender ! Controller.Found(f.nodes, f.peers, f.tokens)
      case Finder.State.Failed =>
        this.log.debug("Sending FAILED to " + sender)
        sender ! Controller.NotFound()
    }
    this.log.debug("Finder state after #iterate: " + f.state)
  }

  /**
   * Processes given response for given transaction.
   *
   * @param response    A [[Response]] message from remote peer.
   * @param transaction A transaction instance that response was received for.
   * @param agent       A reference to [[Agent]] actor.
   */
  private def process(response: Response, transaction: Controller.Transaction, agent: ActorRef) = {
    if (response.id != transaction.remote.id) {
      this.log.warning("Response ID: {}, but requested not had ID: {}", response.id, transaction.remote.id)
    }
    if (transaction.remote.id != Integer160.zero) {
      this.table ! Table.Received(transaction.remote, Message.Kind.Response)
    }
    response match {

      case ping: Response.Ping =>
        transaction.requester ! Controller.Pinged()

      case fn: Response.FindNode =>
        val target = transaction.query.asInstanceOf[Query.FindNode].target
        this.recursions.get(target).foreach { finder =>
          finder.report(transaction.remote, fn.nodes, Nil, Array.empty)
          this.iterate(target, Some(finder), transaction.requester, agent) { () =>
            new Query.FindNode(this.factory.next(), this.reader.id().get, target)
          }
        }

      case gp: Response.GetPeersWithNodes =>
        val infohash = transaction.query.asInstanceOf[Query.GetPeers].infohash
        this.recursions.get(infohash).foreach { finder =>
          finder.report(transaction.remote, gp.nodes, Nil, gp.token)
          this.iterate(infohash, Some(finder), transaction.requester, agent) { () =>
            new Query.GetPeers(this.factory.next(), this.reader.id().get, infohash)
          }
        }

      case gp: Response.GetPeersWithValues =>
        val infohash = transaction.query.asInstanceOf[Query.GetPeers].infohash
        this.recursions.get(infohash).foreach { finder =>
          finder.report(transaction.remote, Nil, gp.values, gp.token)
          this.iterate(infohash, Some(finder), transaction.requester, agent) { () =>
            new Query.GetPeers(this.factory.next(), this.reader.id().get, infohash)
          }
        }

      case ap: Response.AnnouncePeer =>
        transaction.requester ! Controller.PeerAnnounced()
    }
  }

  /// Wraps Reader's `klosest` method adding routers to the output if the table has not enough entries
  private def klosest(n: Int, target: Integer160) = this.reader.klosest(n, target) match {
    case nn: Seq[NodeInfo] if nn.size < n => nn ++ (this.routers map { ra => new NodeInfo(Integer160.zero, ra) })
    case enough: Seq[NodeInfo] => enough
  }

  /// An instance of Responder to handle incoming queries
  private lazy val responder = new Responder(this.K, this.period, this.lifetime, this.reader, this.writer)

  /// Collection of scheduled tasks sending periodic internal commands to self
  private val timerTasks =
    this.context.system.scheduler.schedule(Duration.Zero, this.period, self, Controller.RotateTokens) ::
    this.context.system.scheduler.schedule(this.lifetime, this.lifetime, self, Controller.CleanupPeers) :: Nil

  /// Instantiate transaction ID factory
  private val factory = TIDFactory.random

  /// Collection of pending transactions
  private val transactions = new mutable.HashMap[TID, Controller.Transaction]

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
            routers: Traversable[InetSocketAddress],
            reader: Reader,
            writer: Writer,
            table: ActorRef) =
    Props(classOf[Controller], K, alpha, period, lifetime, routers, reader, writer, table)

  /** Generates [[akka.actor.Props]] instance with most parameters set to their default values */
  def props(routers: Traversable[InetSocketAddress], reader: Reader, writer: Writer, table: ActorRef): Props =
    this.props(8, 3, 5.minutes, 30.minutes, routers, reader, writer, table)

  /**
   * This class defines transaction data.
   *
   * @param query     Original query which initiated transaction.
   * @param remote    Remote node.
   * @param requester An actor which has requested this transaction to begin.
   */
  class Transaction(val query: Query, val remote: NodeInfo, val requester: ActorRef)

  /** Base trait for all events initiated by this actor */
  sealed trait Event

  /* Indicates that Controller is initialized and ready to process requests */
  case object Ready extends Event

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
  case class Found(nodes: Traversable[NodeInfo],
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
  case class Ping(node: NodeInfo) extends Command

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
  case class AnnouncePeer(node: NodeInfo,
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
  case class SendPluginMessage(message: org.abovobo.dht.message.Plugin, node: NodeInfo) extends Command
  case class PutPlugin(pid: PID, plugin: ActorRef) extends Command
  case class RemovePlugin(pid: PID) extends Command
}

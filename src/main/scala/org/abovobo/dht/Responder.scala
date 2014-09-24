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

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import org.abovobo.dht.message.{Message, Query, Response}
import org.abovobo.dht.persistence.Storage

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

/**
 * The responsibility of this class is to generate [[Response]] message
 * from particular incoming [[Query]] message.
 *
 * @param K         Max number of entries to send back to querier.
 * @param period    A period of rotating the token.
 * @param lifetime  A lifetime of the peer info.
 * @param storage   Instance of [[org.abovobo.dht.persistence.Storage]] used to access persisted data.
 * @param table     Reference to [[Table]] actor.
 */
class Responder(K: Int,
                period: FiniteDuration,
                lifetime: FiniteDuration,
                storage: Storage,
                table: ActorRef)
  extends Actor with ActorLogging {

  import this.context.dispatcher

  /** @inheritdoc */
  override def preStart() = {
    this.log.debug("Responder#preStart")
    this.context.watch(this.table)
  }

  /** @inheritdoc */
  override def postStop() = {
    this.log.debug("Responder#postStop")
    this.cancellables.foreach(_.cancel())
    this.context.unwatch(this.table)
  }

  /** @inheritdoc */
  override def receive = {
    case Agent.Received(message, remote) =>
      message match {
        case query: Query =>
          val node = new NodeInfo(query.id, remote)
          this.table ! Table.Received(node, Message.Kind.Query)
          this.sender() ! this.respond(query, node)
        case _ =>
          // do nothing: ignore any other messages from agent
          this.log.warning("Responder only supports Query messages from Agent, received {}", message)
      }
    case Responder.Cleanup => this.cleanup()
    case Responder.Rotate  => this.rotate()
    // To avoid "unhandled message" logging
    case r: Table.Result =>
    // To debug crashes
    case t: Throwable => throw t
  }

  /**
   * Creates an instance of [[Response]] message which is appropriate to
   * an incoming [[Query]] message and sends it to given remote peer.
   *
   * @param query   A query to respond to.
   * @param remote  An address of the remote peer to send response to.
   */
  def respond(query: Query, remote: NodeInfo): Agent.Send = query match {
    case ping: Query.Ping         => this.ping(ping, remote)
    case fn:   Query.FindNode     => this.findNode(fn, remote)
    case gp:   Query.GetPeers     => this.getPeers(gp, remote)
    case ap:   Query.AnnouncePeer => this.announcePeer(ap, remote.address)
  }

  /** Response to `ping` query */
  private def ping(q: Query.Ping, remote: NodeInfo) = {
    // simply send response `ping` message
    Agent.Send(new Response.Ping(q.tid, this.id), remote.address)
  }

  /** Responds to `find_node` query */
  private def findNode(q: Query.FindNode, remote: NodeInfo) = {
    // get `K` closest nodes from own routing table
    val nodes = this.storage.closest(this.K + 1, q.target).filter(_.id != remote.id).take(this.K)
    // now respond with proper message
    Agent.Send(new Response.FindNode(q.tid, this.id, nodes), remote.address)
  }

  /** Responds to `get_peers` query */
  private def getPeers(q: Query.GetPeers, remote: NodeInfo) = {
    // get current token to return to querier
    val token = this.tp.get
    // remember that we sent the token to this remote peer
    this.tokens.getOrElse(remote.address, Nil) match {
      case Nil => this.tokens.put(remote.address, List(token))
      case l => this.tokens.put(remote.address, token :: l)
    }
    // look for peers for the given infohash in the storage
    val peers = this.storage.peers(q.infohash).map(_.address)
    if (peers.nonEmpty) {
      // if there are peers found send them with response
      Agent.Send(new Response.GetPeersWithValues(q.tid, id, token, peers.toSeq), remote.address)
    } else {
      // otherwise send closest K nodes
      val nodes = this.storage.closest(this.K + 1, q.infohash).filter(_.id != remote.id).take(this.K)
      Agent.Send(new Response.GetPeersWithNodes(q.tid, id, token, nodes), remote.address)
    }
  }

  /** Responds to `announce_peer` query */
  private def announcePeer(q: Query.AnnouncePeer, remote: InetSocketAddress): Agent.Send = {
    this.tokens.getOrElse(remote, Nil) match {
      // error: address not found
      case Nil => Agent.Send(
        new message.Error(q.tid, message.Error.ERROR_CODE_PROTOCOL, "Address is missing in token distribution list"),
        remote)
      // ok: address is here
      case l: List[Token] => l.find(_.sameElements(q.token)) match {
        // ok: token value is bound to an address
        case Some(value) =>
          // ok: token value is still valid
          if (this.tp.valid(value)) {
            // put peer into the storage
            this.storage.transaction {
              this.storage.announce(
                q.infohash,
                new Peer(remote.getAddress, if (q.implied) remote.getPort else q.port))
            }
            // send response message back to remote peer
            Agent.Send(new Response.AnnouncePeer(q.tid, id), remote)
          // error: token has expired
          } else {
            Agent.Send(new message.Error(q.tid, message.Error.ERROR_CODE_GENERIC, "Token has expired"), remote)
          }
        // token has not been distributed to given node or has already been cleaned up
        case None => Agent.Send(
          new message.Error(q.tid, message.Error.ERROR_CODE_PROTOCOL, "Given token was not distributed to this node"),
          remote)
      }
    }

  }

  /**
   * This method is executed periodically to rotate tokens (generating a new one, preserving previous) and
   * cleanup `tokens` collection thus removing those token-peer association which has expired.
   */
  private def rotate() = {
    // first rotate
    tp.rotate()
    // then cleanup
    this.tokens.keySet foreach { key =>
      this.tokens.getOrElse(key, Nil).filter(this.tp.valid) match {
        case Nil => this.tokens.remove(key)
        case l => this.tokens.put(key, l)
      }
    }
  }

  /** This method is executed periodically to remote expired infohash-peer associations */
  private def cleanup() = this.storage.transaction(this.storage.cleanup(this.lifetime))

  /** Returns immediate value of node self id */
  private def id = this.storage.id().get

  /// An instance of [[TokenProvider]]
  private val tp = new TokenProvider

  /// Collection of remote peers which received tokens
  private val tokens = new mutable.HashMap[InetSocketAddress, List[Token]]

  /// Collection of scheduled tasks sending periodic internal commands to self
  private val cancellables = List(
    this.context.system.scheduler.schedule(Duration.Zero, this.period, self, Responder.Rotate),
    this.context.system.scheduler.schedule(Duration.Zero, this.lifetime / 5, self, Responder.Cleanup))
}

/** Accompanying object */
object Responder {

  /** Base trait for all commands supported by this actor */
  sealed trait Command

  /** Instructs [[Responder]] to clean old peer info */
  case object Cleanup extends Command

  /** Instructs [[Responder]] to rotate tokens */
  case object Rotate extends Command

  /**
   * Factory which creates RoutingTable Actor Props instance.
   *
   * @param K         Max number of entries per bucket.
   * @param period    A period of rotating the token.
   * @param lifetime  A lifetime of the peer info.
   * @param storage   Instance of [[org.abovobo.dht.persistence.Storage]] used to access persisted data.
   * @param table     Reference to [[Table]] actor.
   *
   * @return          Properly configured Actor Props instance.
   */
  def props(K: Int, period: FiniteDuration, lifetime: FiniteDuration, storage: Storage, table: ActorRef): Props =
    Props(classOf[Responder], K, period, lifetime, storage, table)

  /**
   * Factory which creates RoutingTable Actor Props instance with default values:
   * K = 8, timeout = 15 minutes, delay = 30 seconds, threshold = 3.
   *
   * @param storage    Instance of [[org.abovobo.dht.persistence.Storage]] used to access persisted data.
   * @param table     Reference to [[Table]] actor.
   *
   * @return          Properly configured Actor Props instance.
   */
  def props(storage: Storage, table: ActorRef): Props = this.props(8, 5.minutes, 30.minutes, storage, table)
}

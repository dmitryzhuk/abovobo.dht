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
import akka.actor.Scheduler
import org.abovobo.dht.persistence.{Writer, Reader}
import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, Duration}
import scala.concurrent.ExecutionContext

/**
 * The responsibility of this class is to generate [[org.abovobo.dht.Response]] message
 * from particular incoming [[org.abovobo.dht.Query]] message.
 *
 * The instance of this class is an aggregated sub-part of [[org.abovobo.dht.Controller]] actor.
 *
 * @param K         Max number of entries to send back to querier.
 * @param period    A period of rotating the token.
 * @param lifetime  A lifetime of the peer info.
 * @param reader    Instance of [[org.abovobo.dht.persistence.Reader]] used to access persisted data.
 * @param writer    Instance of [[org.abovobo.dht.persistence.Writer]] to update persisted DHT state.
 * @param scheduler Akka system scheduler derived from owning actor instance.
 * @param ec        General execution context for scheduled tasks derived from owning execution context.
 */
class Responder(K: Int,
                period: FiniteDuration,
                lifetime: FiniteDuration,
                reader: Reader,
                writer: Writer,
                scheduler: Scheduler,
                ec: ExecutionContext)
  extends AutoCloseable{

  /**
   * @inheritdoc
   *
   * Actually cancels timed task which cleans obsolete tokens and stops token provider.
   */
  override def close() = {
    this.cleanupTokensTask.cancel()
    this.cleanupPeersTask.cancel()
    this.tp.stop
  }

  /**
   * Creates an instance of [[org.abovobo.dht.Response]] message which is appropriate to
   * an incoming [[org.abovobo.dht.Query]] message and sends it to given remote peer.
   *
   * @param query   A query to respond to.
   * @param remote  An address of the remote peer to send response to.
   */
  def respond(query: Query, remote: InetSocketAddress): Agent.Send = query match {
    case ping: Query.Ping => this.ping(ping, remote)
    case fn: Query.FindNode => this.findNode(fn, remote)
    case gp: Query.GetPeers => this.getPeers(gp, remote)
    case ap: Query.AnnouncePeer => this.announcePeer(ap, remote)
  }

  /** Response to `ping` query */
  private def ping(q: Query.Ping, remote: InetSocketAddress) = {
    // simply send response `ping` message
    Agent.Send(new Response.Ping(q.tid, this.id), remote)
  }

  /** Responds to `find_node` query */
  private def findNode(q: Query.FindNode, remote: InetSocketAddress) = {
    // get `K` closest nodes from own routing table
    val nodes = this.reader.klosest(this.K, q.target)
    // now respond with proper message
    Agent.Send(new Response.FindNode(q.tid, this.id, nodes), remote)
  }

  /** Responds to `get_peers` query */
  private def getPeers(q: Query.GetPeers, remote: InetSocketAddress) = {
    // get current token to return to querier
    val token = this.tp.get
    // remember that we sent the token to this remote peer
    this.tokens.get(remote).getOrElse(Nil) match {
      case Nil => this.tokens.put(remote, List(token))
      case l => this.tokens.put(remote, token :: l)
    }
    // look for peers for the given infohash in the storage
    val peers = this.reader.peers(q.infohash)
    if (!peers.isEmpty) {
      // if there are peers found send them with response
      Agent.Send(new Response.GetPeersWithValues(q.tid, id, token, peers.toSeq), remote)
    } else {
      // otherwise send closest K nodes
      val nodes = this.reader.klosest(this.K, q.infohash)
      Agent.Send(new Response.GetPeersWithNodes(q.tid, id, token, nodes), remote)
    }
  }

  /** Responds to `announce_peer` query */
  private def announcePeer(q: Query.AnnouncePeer, remote: InetSocketAddress): Agent.Send = {
    this.tokens.get(remote).getOrElse(Nil) match {
      // error: address not found
      case Nil => Agent.Send(
        new Error(q.tid, Error.ERROR_CODE_PROTOCOL, "Address is missing in token distribution list"),
        remote)
      // ok: address is here
      case l: List[Token] => l.find(_.sameElements(q.token)) match {
        // ok: token value is bound to an address
        case Some(value) =>
          // ok: token value is still valid
          if (this.tp.valid(value)) {
            // put peer into the storage
            this.writer.transaction {
              this.writer.announce(
                q.infohash,
                new Peer(remote.getAddress, if (q.implied) remote.getPort else q.port))
            }
            // send response message back to remote peer
            Agent.Send(new Response.AnnouncePeer(q.tid, id), remote)
          // error: token has expired
          } else {
            Agent.Send(new Error(q.tid, Error.ERROR_CODE_GENERIC, "Token has expired"), remote)
          }
        // token has not been distributed to given node or has already been cleaned up
        case None => Agent.Send(
          new Error(q.tid, Error.ERROR_CODE_PROTOCOL, "Given token was not distributed to this node"),
          remote)
      }
    }

  }

  /** Returns immediate value of node self id */
  private def id = this.reader.id().get

  /**
   * This method is executed periodically to cleanup `tokens` collection
   * thus removing those token-peer association which has expired.
   */
  private def cleanupTokens() = this.tokens.keySet foreach { key =>
    this.tokens.get(key).getOrElse(Nil).filter(this.tp.valid) match {
      case Nil => this.tokens.remove(key)
      case l => this.tokens.put(key, l)
    }
  }

  /** This method is executed periodically to remote expired infohash-peer associations */
  private def cleanupPeers() =
    this.writer.cleanup(this.lifetime)

  /// Initializes token provider
  private val tp = new TokenProvider(this.period, this.scheduler, this.ec)

  /// Collection of remote peers which received tokens
  private val tokens = new mutable.HashMap[InetSocketAddress, List[Token]]

  /// Periodic task which cleans up `tokens` collection
  private val cleanupTokensTask =
    this.scheduler.schedule(this.period, this.period * 2)(this.cleanupTokens())(this.ec)

  /// Periodic task which cleans up `peers` persistent collection
  private val cleanupPeersTask =
    this.scheduler.schedule(Duration.Zero, this.lifetime)(this.cleanupPeers())(this.ec)

}

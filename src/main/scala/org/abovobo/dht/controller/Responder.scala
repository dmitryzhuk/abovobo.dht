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

import org.abovobo.dht._
import org.abovobo.dht.message.{Query, Response}
import org.abovobo.dht.persistence.{Reader, Writer}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
 * The responsibility of this class is to generate [[Response]] message
 * from particular incoming [[Query]] message.
 *
 * The instance of this class is an aggregated sub-part of [[Controller]] actor.
 *
 * @param K         Max number of entries to send back to querier.
 * @param period    A period of rotating the token.
 * @param lifetime  A lifetime of the peer info.
 * @param reader    Instance of [[org.abovobo.dht.persistence.Reader]] used to access persisted data.
 * @param writer    Instance of [[org.abovobo.dht.persistence.Writer]] to update persisted DHT state.
 */
class Responder(K: Int,
                period: FiniteDuration,
                lifetime: FiniteDuration,
                reader: Reader,
                writer: Writer) {
  /**
   * Creates an instance of [[Response]] message which is appropriate to
   * an incoming [[Query]] message and sends it to given remote peer.
   *
   * @param query   A query to respond to.
   * @param remote  An address of the remote peer to send response to.
   */
  def respond(query: Query, remote: Node): Agent.Send = query match {
    case ping: Query.Ping => this.ping(ping, remote)
    case fn: Query.FindNode => this.findNode(fn, remote)
    case gp: Query.GetPeers => this.getPeers(gp, remote)
    case ap: Query.AnnouncePeer => this.announcePeer(ap, remote.address)
  }

  /** Response to `ping` query */
  private def ping(q: Query.Ping, remote: Node) = {
    // simply send response `ping` message
    Agent.Send(new Response.Ping(q.tid, this.id), remote.address)
  }

  /** Responds to `find_node` query */
  private def findNode(q: Query.FindNode, remote: Node) = {
    // get `K` closest nodes from own routing table
    val nodes = this.reader.klosest(this.K + 1, q.target).filter(_.id != remote.id).take(this.K)
    // now respond with proper message
    Agent.Send(new Response.FindNode(q.tid, this.id, nodes), remote.address)
  }

  /** Responds to `get_peers` query */
  private def getPeers(q: Query.GetPeers, remote: Node) = {
    // get current token to return to querier
    val token = this.tp.get
    // remember that we sent the token to this remote peer
    this.tokens.getOrElse(remote.address, Nil) match {
      case Nil => this.tokens.put(remote.address, List(token))
      case l => this.tokens.put(remote.address, token :: l)
    }
    // look for peers for the given infohash in the storage
    val peers = this.reader.peers(q.infohash)
    if (peers.nonEmpty) {
      // if there are peers found send them with response
      Agent.Send(new Response.GetPeersWithValues(q.tid, id, token, peers.toSeq), remote.address)
    } else {
      // otherwise send closest K nodes
      val nodes = this.reader.klosest(this.K + 1, q.infohash).filter(_.id != remote.id).take(this.K)
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
            this.writer.transaction {
              this.writer.announce(
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

  /** Returns immediate value of node self id */
  private def id = this.reader.id().get

  /**
   * This method is executed periodically to rotate tokens (generating a new one, preserving previous) and
   * cleanup `tokens` collection thus removing those token-peer association which has expired.
   */
  def rotateTokens() = {
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
  def cleanupPeers() = this.writer.cleanup(this.lifetime)

  /// An instance of [[TokenProvider]]
  private val tp = new TokenProvider

  /// Collection of remote peers which received tokens
  private val tokens = new mutable.HashMap[InetSocketAddress, List[Token]]
}

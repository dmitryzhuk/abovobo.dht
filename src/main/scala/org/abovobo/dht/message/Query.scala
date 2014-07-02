/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.message

import org.abovobo.dht.{TID, Token}
import org.abovobo.integer.Integer160

/**
 * Abstract query message.
 *
 * @param tid   Transaction identifier.
 * @param id    Sending node identifier.
 * @param name  Message name ('ping', 'find_node', 'get_peers', 'announce_peer')
 */
abstract class Query(tid: TID, id: Integer160, val name: String) extends Normal(tid, 'q', id) {

  /** @inheritdoc */
  override def toString = "Q: [" + name + "], tid: " + tid + ", self: " + id
}

/** Accompanying object. */
object Query {

  /// Names of possible query messages
  ///
  val QUERY_NAME_PING = "ping"
  val QUERY_NAME_FIND_NODE = "find_node"
  val QUERY_NAME_GET_PEERS = "get_peers"
  val QUERY_NAME_ANNOUNCE_PEER = "announce_peer"

  /**
   * Represents `ping` query message.
   *
   * @param tid   Transaction identifier.
   * @param id    Sending node identifier.
   */
  class Ping(tid: TID, id: Integer160)
    extends Query(tid, id, QUERY_NAME_PING) {

    /** @inheritdoc */
    override def toBencodedString =
      "d1:ad2:id20:`" + this.id.toHexString + "`e1:q4:ping1:t2:" + this.tid.toString + "1:y1:qe"
  }

  /**
   * Represents `find_node` query message.
   *
   * @param tid     Transaction identifier.
   * @param id      Sending node identifier.
   * @param target  An id of hypotetical node to find.
   */
  class FindNode(tid: TID, id: Integer160,
                 val target: Integer160)
    extends Query(tid, id, QUERY_NAME_FIND_NODE) {

    /** @inheritdoc */
    override def toBencodedString =
      "d1:ad2:id20:`" + this.id.toHexString + "`6:target20:`" + this.target.toHexString +
        "`e1:q9:find_node1:t2:" + this.tid.toString + "1:y1:qe"

    /** @inheritdoc */
    override def toString = super.toString + ", target: " + target
  }

  /**
   * Represents `get_peers` query message.
   *
   * @param tid       Transaction identifier.
   * @param id        Sending node identifier.
   * @param infohash  An infohash of torrent to get peers for.
   */
  class GetPeers(tid: TID, id: Integer160,
                 val infohash: Integer160)
    extends Query(tid, id, QUERY_NAME_GET_PEERS) {

    /** @inheritdoc */
    override def toBencodedString =
      "d1:ad2:id20:`" + this.id.toHexString + "`9:info_hash20:`" + this.infohash.toHexString +
        "`e1:q9:get_peers1:t2:" + this.tid.toString + "1:y1:qe"

    /** @inheritdoc */
    override  def toString = super.toString + ", infohash: " + infohash
  }

  /**
   * Announces own peer as the one which is bound to particular torrent.
   *
   * @param tid       Transaction identifier.
   * @param id        Sending node identifier.
   * @param infohash  An infohash of torrent to announce itself as a peer for.
   * @param port      A port at which peer is listening for incoming connections.
   * @param token     A token received in previos response to `get_peers` query.
   * @param implied   Flag, indicating if given port must be taken into account.
   */
  class AnnouncePeer(tid: TID, id: Integer160,
                     val infohash: Integer160, val port: Int, val token: Token, val implied: Boolean)
    extends Query(tid, id, QUERY_NAME_ANNOUNCE_PEER) {

    /** @inheritdoc */
    override def toBencodedString =
      "d1:ad2:id20:`" + this.id.toHexString + "`" +
        "12:implied_porti" + (if (implied) 1 else 0) + "e" +
        "9:info_hash20:`" + this.infohash.toHexString + "`" +
        "4:porti" + this.port + "e" +
        "5:token" + this.token.length + ":`" + org.abovobo.conversions.Hex.ba2hex(this.token) + "`" +
        "e1:q13:announce_peer1:t2:" + this.tid.toString + "1:y1:qe"

    /** @inheritdoc */
    override def toString = super.toString +
      ", infohash: " + infohash +
      ", port: " + port +
      ", token: " + org.abovobo.conversions.Hex.ba2hex(this.token)
  }
}

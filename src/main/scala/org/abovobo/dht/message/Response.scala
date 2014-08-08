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

import org.abovobo.dht.{TID, NodeInfo, Endpoint, Token, Peer}
import org.abovobo.integer.Integer160

/**
 * Abstract response message.
 *
 * @param tid Transaction identifier.
 * @param id  Sending node identifier.
 */
abstract class Response(tid: TID, id: Integer160) extends Normal(tid, 'r', id) {

  /** @inheritdoc */
  override def toString = "tid: " + tid + ", self: " + id
}

object Response {

  /**
   * Message in response to `ping` query.
   *
   * @param tid Transaction identifier.
   * @param id  Sending node identifier.
   */
  class Ping(tid: TID, id: Integer160)
    extends Response(tid, id) {

    /** @inheritdoc */
    override def toString = "R: [ping] -> " + super.toString

    /** @inheritdoc */
    override def toBencodedString =
      "[ping] -> d1:rd2:id20:`" + this.id.toHexString + "`e1:t2:" + this.tid.toString + "1:y1:re"


  }

  /**
   * Message in response to `find_node` query.
   *
   * @param tid   Transaction identifier.
   * @param id    Sending node identifier.
   * @param nodes Collection of nodes with ids closest to requested target.
   */
  class FindNode(tid: TID, id: Integer160, val nodes: Seq[NodeInfo])
    extends Response(tid, id) {

    /** @inheritdoc */
    override def toString = "R: [find_node] -> " + super.toString + ", nodes: " + nodes.mkString("(", ",", ")")

    /** @inheritdoc */
    override def toBencodedString =
      "[find_node] -> " +
        "d1:rd2:id20:`" + this.id.toHexString + "`" +
        "5:nodes" + (this.nodes.length * 26) + ":`" +
        org.abovobo.conversions.Hex.ba2hex(this.nodes
          .map(node => node.id.toArray ++ Endpoint.isa2ba(node.address))
          .foldLeft(Array[Byte]()){ (a, node) => a ++ node }) + "`" +
        "e1:t2:" + this.tid.toString + "1:y1:re"
  }

  /**
   * Abstract class representing message in response to `get_peers` query.
   *
   * @param tid   Transaction identifier.
   * @param id    Sending node identifier.
   * @param token Special token which then must be used in `announce_peer` query.
   */
  abstract class GetPeers(tid: TID, id: Integer160, val token: Token)
    extends Response(tid, id)

  /**
   * Concrete variant of [[Query.GetPeers]] class, representing
   * response with collection of nodes, with identifiers closest to requested infohash.
   *
   * @param tid   Transaction identifier.
   * @param id    Sending node identifier.
   * @param token Special token which then must be used in `announce_peer` query.
   * @param nodes Collection of nodes with ids closest to requested infohash.
   */
  class GetPeersWithNodes(tid: TID, id: Integer160, token: Token, val nodes: Seq[NodeInfo])
    extends GetPeers(tid, id, token) {

    /** @inheritdoc */
    override def toString = "R: [get_peers] -> " + super.toString + ", nodes: " + nodes.mkString("(", ",", ")")

    /** @inheritdoc */
    override def toBencodedString =
      "[get_peers] -> " +
        "d1:rd2:id20:`" + this.id.toHexString + "`" +
        "5:nodes" + (this.nodes.length * 26) + ":`" +
        org.abovobo.conversions.Hex.ba2hex(this.nodes
          .map(node => node.id.toArray ++ Endpoint.isa2ba(node.address))
          .reduceLeft((left, node) => left ++ node)) + "`" +
        "5:token" + this.token.length + ":`" + org.abovobo.conversions.Hex.ba2hex(this.token) + "`" +
        "e1:t2:" + this.tid.toString + "1:y1:re"
  }

  /**
   * Concrete variant of [[Response.GetPeers]] class, representing
   * response with list of peers at requested torrent.
   *
   * @param tid     Transaction identifier.
   * @param id      Sending node identifier.
   * @param token   Special token which then must be used in `announce_peer` query.
   * @param values  Collection of peers at requested torrent.
   */
  class GetPeersWithValues(tid: TID, id: Integer160, token: Token, val values: Seq[Peer])
    extends GetPeers(tid, id, token) {

    /** @inheritdoc */
    override def toString = "R: [get_peers] -> " + super.toString + ", peers: " + values.mkString("(", ",", ")")

    /** @inheritdoc */
    override def toBencodedString =
      "[get_peers] -> " +
        "d1:rd2:id20:`" + this.id.toHexString + "`" +
        "5:token" + this.token.length + ":`" + org.abovobo.conversions.Hex.ba2hex(this.token) + "`" +
        "6:values" + (this.values.length * 6) + ":`" +
        org.abovobo.conversions.Hex.ba2hex(this.values
          .map(Endpoint.isa2ba)
          .reduceLeft((left, peer) => left ++ peer)) + "`" +
        "e1:t2:" + this.tid.toString + "1:y1:re"
  }

  /**
   * Message in response to `announce_peer` query.
   *
   * @param tid Transaction identifier.
   * @param id  Sending node identifier.
   */
  class AnnouncePeer(tid: TID, id: Integer160)
    extends Response(tid, id) {

    /** @inheritdoc */
    override def toString = "R: [announce_peer] -> " + super.toString

    /** @inheritdoc */
    override def toBencodedString =
      "[announce_peer] -> d1:rd2:id20:`" + this.id.toHexString + "`e1:t2:" + this.tid.toString + "1:y1:re"
  }
}


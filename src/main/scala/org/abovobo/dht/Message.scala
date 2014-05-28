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
import org.abovobo.conversions.Bencode

/**
 * Base abstract class defining general contract for any message which
 * actually represents sendable Kademlia packet.
 *
 * @param tid Transaction identifier.
 * @param y   Message kind: 'e' for error, 'q' for query, 'r' for response.
 */
abstract class Message(val tid: TID, val y: Char) {
  def kind: Message.Kind.Value = this.y match {
    case 'q' => Message.Kind.Query
    case 'r' => Message.Kind.Response
    case 'p' => Message.Kind.PluginMessage
    case 'e' => Message.Kind.Error
  }
}

/** Accompanying object */
object Message {

  /**
   * This enumeration defines possible kinds of network message:
   *
   * Query means that remote node has sent us a query message,
   * Response means that remote node has replied with correct message to our query,
   * Error means that remote node has replied with error message to our query,
   * Fail  means that remote node failed to reply in timely manner.
   */
  object Kind extends Enumeration {
    type Kind = Value
    val Query, Response, PluginMessage, Error, Fail = Value
  }

}

/**
 * Concrete [[org.abovobo.dht.Message]] implementation, representing error.
 *
 * @param tid     Transaction identifier.
 * @param code    Error code.
 * @param message Error message.
 */
class Error(tid: TID, val code: Long, val message: String) extends Message(tid, 'e') {
  override def toString =
    "d1:eli" + this.code + "e" + this.message.length + ":" + this.message + "e1:t2:" + this.tid.toString + "1:y1:ee"
}

/**
 * Accompanying object.
 *
 * Defines error constants as they are descibed in [[http://www.bittorrent.org/beps/bep_0005.html#errors]]
 */
object Error {
  val ERROR_CODE_GENERIC = 201
  val ERROR_MESSAGE_GENERIC = "Generic Error"

  val ERROR_CODE_SERVER = 202
  val ERROR_MESSAGE_SERVER = "Server Error"

  val ERROR_CODE_PROTOCOL = 203
  val ERROR_MESSAGE_PROTOCOL = "Protocol Error"

  val ERROR_CODE_UNKNOWN = 204
  val ERROR_MESSAGE_UNKNOWN = "Method Unknown"
}

/**
 * Abstract class representing normal (in opposite to error) message.
 *
 * @param tid Transaction identifier.
 * @param y   Message kind: 'e' for error, 'q' for query, 'r' for response.
 * @param id  Sending node identifier.
 */
abstract class Normal(tid: TID, y: Char, val id: Integer160) extends Message(tid, y)

/**
 * Abstract query message.
 *
 * @param tid   Transaction identifier.
 * @param id    Sending node identifier.
 * @param name  Message name ('ping', 'find_node', 'get_peers', 'announce_peer')
 */
abstract class Query(tid: TID, id: Integer160, val name: String) extends Normal(tid, 'q', id)

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

    override def toString =
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

    override def toString =
      "d1:ad2:id20:`" + this.id.toHexString + "`6:target20:`" + this.target.toHexString +
        "`e1:q9:find_node1:t2:" + this.tid.toString + "1:y1:qe"

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

    override def toString =
      "d1:ad2:id20:`" + this.id.toHexString + "`9:info_hash20:`" + this.infohash.toHexString +
        "`e1:q9:get_peers1:t2:" + this.tid.toString + "1:y1:qe"

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

    override def toString =
      "d1:ad2:id20:`" + this.id.toHexString + "`" +
        "12:implied_porti" + (if (implied) 1 else 0) + "e" +
        "9:info_hash20:`" + this.infohash.toHexString + "`" +
        "4:porti" + this.port + "e" +
        "5:token" + this.token.length + ":`" + org.abovobo.conversions.Hex.ba2hex(this.token) + "`" +
        "e1:q13:announce_peer1:t2:" + this.tid.toString + "1:y1:qe"

  }
}

/**
 * Abstract response message.
 *
 * @param tid Transaction identifier.
 * @param id  Sending node identifier.
 */
abstract class Response(tid: TID, id: Integer160) extends Normal(tid, 'r', id)

object Response {

  /**
   * Message in response to `ping` query.
   *
   * @param tid Transaction identifier.
   * @param id  Sending node identifier.
   */
  class Ping(tid: TID, id: Integer160)
    extends Response(tid, id) {

    override def toString =
      "[ping] -> d1:rd2:id20:`" + this.id.toHexString + "`e1:t2:" + this.tid.toString + "1:y1:re"

  }

  /**
   * Message in response to `find_node` query.
   *
   * @param tid   Transaction identifier.
   * @param id    Sending node identifier.
   * @param nodes Collection of nodes with ids closest to requested target.
   */
  class FindNode(tid: TID, id: Integer160, val nodes: Seq[Node])
    extends Response(tid, id) {

    override def toString =
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
   * Concrete variant of [[org.abovobo.dht.Query.GetPeers]] class, representing
   * response with collection of nodes, with identifiers closest to requested infohash.
   *
   * @param tid   Transaction identifier.
   * @param id    Sending node identifier.
   * @param token Special token which then must be used in `announce_peer` query.
   * @param nodes Collection of nodes with ids closest to requested infohash.
   */
  class GetPeersWithNodes(tid: TID, id: Integer160, token: Token, val nodes: Seq[Node])
    extends GetPeers(tid, id, token) {

    override def toString =
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
   * Concrete variant of [[org.abovobo.dht.Response.GetPeers]] class, representing
   * response with list of peers at requested torrent.
   *
   * @param tid     Transaction identifier.
   * @param id      Sending node identifier.
   * @param token   Special token which then must be used in `announce_peer` query.
   * @param values  Collection of peers at requested torrent.
   */
  class GetPeersWithValues(tid: TID, id: Integer160, token: Token, val values: Seq[Peer])
    extends GetPeers(tid, id, token) {

    override def toString =
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

    override def toString =
      "[announce_peer] -> d1:rd2:id20:`" + this.id.toHexString + "`e1:t2:" + this.tid.toString + "1:y1:re"

  }
}

abstract class PluginMessage(tid: TID, id: Integer160, val pluginId: Plugin.PID, val payloadBytes: Array[Byte]) extends Normal(tid, 'p', id) {

}


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

/**
 * Created by dmitryzhuk on 06.01.14.
 */
object Message {

  /**
   * This enumeration defines possible kinds of network message:
   *
   * Query means that remote node has sent us a query message,
   * Reply means that remote node has replied with correct message to our query,
   * Error means that remote node has replied with error message to our query,
   * Fail  means that remote node failed to reply.
   */
  object Kind extends Enumeration {
    type Kind = Value
    val Query, Reply, Error, Fail = Value
  }

}

abstract class Message(val tid: TID, val y: Char) {
  def kind: Message.Kind.Value = this.y match {
    case 'e' => Message.Kind.Error
    case 'q' => Message.Kind.Query
    case 'r' => Message.Kind.Reply
  }
}

class Error(tid: TID, val code: Long, val message: String) extends Message(tid, 'e')

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

abstract class Normal(tid: TID, y: Char, val id: Integer160) extends Message(tid, y)

abstract class Query(tid: TID, id: Integer160, val name: String) extends Normal(tid, 'q', id)

object Query {

  val QUERY_NAME_PING = "ping"
  val QUERY_NAME_FIND_NODE = "find_node"
  val QUERY_NAME_GET_PEERS = "get_peers"
  val QUERY_NAME_ANNOUNCE_PEER = "announce_peer"

  class Ping(tid: TID, id: Integer160)
    extends Query(tid, id, QUERY_NAME_PING)

  class FindNode(tid: TID, id: Integer160,
                 val target: Integer160)
    extends Query(tid, id, QUERY_NAME_FIND_NODE)

  class GetPeers(tid: TID, id: Integer160,
                 val infohash: Integer160)
    extends Query(tid, id, QUERY_NAME_GET_PEERS)

  class AnnouncePeer(tid: TID, id: Integer160,
                     val infohash: Integer160, val port: Int, val token: Array[Byte], val implied: Boolean)
    extends Query(tid, id, QUERY_NAME_ANNOUNCE_PEER)
}

abstract class Response(tid: TID, id: Integer160) extends Normal(tid, 'r', id)

object Response {
  class Ping(tid: TID, id: Integer160)
    extends Response(tid, id)

  class FindNode(tid: TID, id: Integer160, val nodes: IndexedSeq[Node])
    extends Response(tid, id)

  abstract class GetPeers(tid: TID, id: Integer160, val token: Array[Byte])
    extends Response(tid, id)

  class GetPeersWithNodes(tid: TID, id: Integer160, token: Array[Byte], val nodes: IndexedSeq[Node])
    extends GetPeers(tid, id, token)

  class GetPeersWithValues(tid: TID, id: Integer160, token: Array[Byte], val values: IndexedSeq[Peer])
    extends GetPeers(tid, id, token)

  class AnnouncePeer(tid: TID, id: Integer160)
    extends Response(tid, id)
}

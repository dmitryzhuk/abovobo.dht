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

import akka.util.{ByteStringBuilder, ByteString}
import org.abovobo.conversions.Bencode
import org.abovobo.dht
import org.abovobo.dht._
import org.abovobo.integer.Integer160

import scala.collection.mutable.ListBuffer

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
    case 'p' => Message.Kind.Plugin
    case 'e' => Message.Kind.Error
  }

  /** Returns exploded stringified bencode representation */
  def toBencodedString: String
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
    val Query, Response, Plugin, Error, Fail = Value
  }


  /**
   * Represents exception which may happen during packet parsing process.
   * Note that this exception will normally result in sending [[message.Error]]
   * message back to remote peer which sent us a packet. It means, that effectively
   * this exception will only be thrown after bencoded packet already decoded successfully
   * and transaction identifier located correctly.
   *
   * @param error An error message to send back to remote peer.
   */
  class ParsingException(val error: message.Error) extends Exception

  /**
   * Parses given [[akka.util.ByteString]] producing instance of corresponding
   * [[Message]] type. If something goes wrong, throws
   * [[Message.ParsingException]] with corresponding
   * [[message.Error]] message which can be sent to remote party.
   *
   * @param data  [[akka.util.ByteString]] instance representing UDP packet received
   *              from remote peer.
   * @param query function which returns query by Transaction Id; used to properly parse
   *              Response message.
   * @return      [[Message]] instance of proper type.
   */
  def parse(data: ByteString)(query: (TID => Option[Query])): Message = {

    import org.abovobo.dht.Endpoint._

    val dump = Bencode.decode(data).toIndexedSeq
    val n = dump.length

    val tid = dump(n - 4) match {
      case Bencode.Bytestring(value) => new TID(value)
      case _ => throw new IllegalArgumentException("Failed to retrieve transaction id")
    }

    def xthrow(code: Int, message: String) =
      throw new Message.ParsingException(new dht.message.Error(tid, code, message))

    def array(event: Bencode.Event): Array[Byte] = event match {
      case Bencode.Bytestring(value) => value
      case _ => xthrow(message.Error.ERROR_CODE_PROTOCOL, "Malformed packet")
    }

    def integer160(event: Bencode.Event): Integer160 = event match {
      case Bencode.Bytestring(value) => new Integer160(value)
      case _ => xthrow(message.Error.ERROR_CODE_PROTOCOL, "Malformed packet")
    }

    def string(event: Bencode.Event): String = event match {
      case Bencode.Bytestring(value) => new String(value, "UTF-8")
      case _ => xthrow(message.Error.ERROR_CODE_PROTOCOL, "Malformed packet")
    }

    def byteString(event: Bencode.Event): ByteString = event match {
      case Bencode.Bytestring(value) => ByteString(value)
      case _ => xthrow(message.Error.ERROR_CODE_PROTOCOL, "Malformed packet")
    }

    def nodes(event: Bencode.Event): IndexedSeq[NodeInfo] = event match {
      case Bencode.Bytestring(value) =>
        val sz = Integer160.bytesize
        val ez = sz + Endpoint.IPV4_ADDR_SIZE + 2
        val n = value.length / ez
        for (i <- 0 until n) yield new NodeInfo(new Integer160(value.drop(i * ez).take(sz)), value.drop(i * ez).drop(sz).take(Endpoint.IPV4_ADDR_SIZE + 2))
      case _ => xthrow(message.Error.ERROR_CODE_PROTOCOL, "Malformed packet")
    }

    def peers(events: IndexedSeq[Bencode.Event]): IndexedSeq[Peer] = events map {
      case Bencode.Bytestring(value) => Endpoint.ba2isa(value)
      case _ => xthrow(message.Error.ERROR_CODE_PROTOCOL, "Malformed packet")
    }

    def integer(event: Bencode.Event): Long = event match {
      case Bencode.Integer(value) => value
      case _ => xthrow(message.Error.ERROR_CODE_PROTOCOL, "Malformed packet")
    }

    dump(n - 2) match {
      case Bencode.Bytestring(value) => value(0) match {
        case 'e' =>
          new message.Error(tid, integer(dump(n - 8)), string(dump(n - 7)))
        case 'q' =>
          string(dump(n - 6)) match {
            case Query.QUERY_NAME_PING =>
              new Query.Ping(tid, integer160(dump(n - 9)))
            case Query.QUERY_NAME_FIND_NODE =>
              new Query.FindNode(tid, integer160(dump(n - 11)), integer160(dump(n - 9)))
            case Query.QUERY_NAME_GET_PEERS =>
              new Query.GetPeers(tid, integer160(dump(n - 11)), integer160(dump(n - 9)))
            case Query.QUERY_NAME_ANNOUNCE_PEER =>
              val variables = string(dump(n - 16)) match {
                case "id" => integer160(dump(n - 15)) -> false
                case "implied_port" => integer160(dump(n - 17)) -> (integer(dump(n - 15)) > 0)
              }
              new Query.AnnouncePeer(
                tid = tid,
                id = variables._1,
                infohash = integer160(dump(n - 13)),
                port = integer(dump(n - 11)).toInt,
                token = array(dump(n - 9)),
                implied = variables._2)
            case _ => xthrow(message.Error.ERROR_CODE_UNKNOWN, "Unknown query")
          }
        case 'r' =>
          query(tid) match {
            case Some(q) =>
              q match {
                case p: Query.Ping =>
                  new Response.Ping(tid, integer160(dump(n - 7)))
                case fn: Query.FindNode =>
                  new Response.FindNode(tid, integer160(dump(n - 9)), nodes(dump(n - 7)))
                case gp: Query.GetPeers =>
                  string(dump(5)) match {
                    case "nodes" =>
                      new Response.GetPeersWithNodes(
                        tid = tid,
                        id = integer160(dump(4)),
                        token = array(dump(8)),
                        nodes = nodes(dump(6)))
                    case "token" =>
                      new Response.GetPeersWithValues(
                        tid = tid,
                        id = integer160(dump(4)),
                        token = array(dump(6)),
                        values = peers(dump.slice(9, n - 7)))
                    case _ => xthrow(message.Error.ERROR_CODE_PROTOCOL, "Malformed packet")
                  }
                case ap: Query.AnnouncePeer =>
                  new Response.AnnouncePeer(tid, integer160(dump(n - 7)))
                case _ => xthrow(message.Error.ERROR_CODE_GENERIC, "Unknown corresponding query type")
              }
            case None =>
              xthrow(message.Error.ERROR_CODE_PROTOCOL, "Invalid transaction id")
          }
        case 'p' =>
          val id = integer160(dump(n - 9))
          val pid = new PID(integer(dump(n - 8)))
          val payload = byteString(dump(n - 7))
          new message.Plugin(tid, id, pid, payload) {}

        case _ => xthrow(message.Error.ERROR_CODE_UNKNOWN, "Unknown method")
      }
      case _ => xthrow(message.Error.ERROR_CODE_UNKNOWN, "Malformed packet")
    }
  }

  /**
   * Serializes given message into a packet sendable via network.
   *
   * @param message A message to serialize
   * @return [[akka.util.ByteString]] instance which can be sent via network.
   */
  // TODO Remove redundant method soon
  def _serialize(message: Message): ByteString = {
    val buf = new ByteStringBuilder()
    buf += 'd'

    message match {
      case error: dht.message.Error =>
        // "e" -> list(code, message)
        buf += '1' += ':' += 'e'
        buf += 'l'
        buf += 'i' ++= error.code.toString.getBytes("UTF-8") += 'e'
        buf ++= error.message.length.toString.getBytes("UTF-8") += ':' ++= error.message.getBytes("UTF-8")
        buf += 'e'

      case query: Query =>
        // "a" -> dictionary(<arguments>)
        buf += '1' += ':' += 'a'
        buf += 'd'
        // "id" -> query.id
        buf += '2' += ':' += 'i' += 'd'
        buf += '2' += '0' += ':' ++= query.id.toArray
        query match {
          case q: Query.Ping =>
          // no other arguments
          case q: Query.FindNode =>
            // "target" -> query.target
            buf += '6' += ':' ++= "target".getBytes("UTF-8")
            buf += '2' += '0' += ':' ++= q.target.toArray
          case q: Query.GetPeers =>
            // "info_hash" -> query.infohash
            buf += '9' += ':' ++= "info_hash".getBytes("UTF-8")
            buf += '2' += '0' += ':' ++= q.infohash.toArray
          case q: Query.AnnouncePeer =>
            // "implied_port" -> query.implied (1 or 0)
            buf += '1' += '2' += ':' ++= "implied_port".getBytes("UTF-8")
            buf += 'i' += (if (q.implied) '1' else '0') += 'e'
            // "info_hash" -> query.infohash
            buf += '9' += ':' ++= "info_hash".getBytes("UTF-8")
            buf += '2' += '0' += ':' ++= q.infohash.toArray
            // "port" -> query.port
            buf += '4' += ':' += 'p' += 'o' += 'r' += 't'
            buf += 'i' ++= q.port.toString.getBytes("UTF-8") += 'e'
            // "token" -> query.token
            buf += '5' += ':' ++= "token".getBytes("UTF-8")
            buf ++= q.token.length.toString.getBytes("UTF-8") += ':' ++= q.token
        }
        buf += 'e'
        // "q" -> query.name
        buf += '1' += ':' += 'q'
        buf ++= query.name.length.toString.getBytes("UTF-8") += ':' ++= query.name.getBytes("UTF-8")

      case response: Response =>
        // "r" -> dictionary(<arguments>)
        buf += '1' += ':' += 'r'
        buf += 'd'
        // "id" -> response.id
        buf += '2' += ':' += 'i' += 'd'
        buf += '2' += '0' += ':' ++= response.id.toArray
        response match {
          case r: Response.Ping =>
          // no other arguments
          case r: Response.FindNode =>
            // "nodes" -> response.nodes
            buf += '5' += ':' ++= "nodes".getBytes("UTF-8")
            val nodes = r.nodes.foldLeft[Array[Byte]](Array.empty) { (array: Array[Byte], node: NodeInfo) =>
              array ++ node.id.toArray ++ Endpoint.isa2ba(node.address)
            }
            buf ++= nodes.length.toString.getBytes("UTF-8") += ':' ++= nodes
          case r: Response.GetPeersWithNodes =>
            // "nodes" -> response.nodes
            buf += '5' += ':' ++= "nodes".getBytes("UTF-8")
            val nodes = r.nodes.foldLeft[Array[Byte]](Array.empty) { (array: Array[Byte], node: NodeInfo) =>
              array ++ node.id.toArray ++ Endpoint.isa2ba(node.address)
            }
            buf ++= nodes.length.toString.getBytes("UTF-8") += ':' ++= nodes
            // "token" -> response.token
            buf += '5' += ':' ++= "token".getBytes("UTF-8")
            buf ++= r.token.length.toString.getBytes("UTF-8") += ':' ++= r.token
          case r: Response.GetPeersWithValues =>
            // "token" -> response.token
            buf += '5' += ':' ++= "token".getBytes("UTF-8")
            buf ++= r.token.length.toString.getBytes("UTF-8") += ':' ++= r.token
            // "values" -> list(peers)
            buf += '6' += ':' ++= "values".getBytes("UTF-8")
            buf += 'l'
            r.values foreach { peer =>
              val array = Endpoint.isa2ba(peer)
              buf ++= array.length.toString.getBytes("UTF-8") += ':' ++= array
            }
            buf += 'e'
          case r: Response.AnnouncePeer =>
          // no other arguments
        }
        buf += 'e'

      case p: org.abovobo.dht.message.Plugin =>
        // 'p' -> list(nodeId, pluginId, message)
        buf += '1' += ':' += 'p'
        buf += 'l'
        buf += '2' += '0' += ':' ++= p.id.toArray
        buf += 'i' ++= p.pid.toString.getBytes("UTF-8") += 'e'
        buf ++= p.payload.length.toString.getBytes("UTF-8") += ':' ++= p.payload
        buf += 'e'

    }
    buf += '1' += ':' += 't'
    buf ++= message.tid.toArray.length.toString.getBytes("UTF-8") += ':' ++= message.tid.toArray
    buf += '1' += ':' += 'y'
    buf += '1' += ':' += message.y.toByte

    buf += 'e'
    buf.result()
  }

  /**
   * Serializes given message into a packet sendable via network.
   *
   * @param message A message to serialize
   * @return [[akka.util.ByteString]] instance which can be sent via network.
   */
  def serialize(message: Message): ByteString = {
    val events = new ListBuffer[Bencode.Event]
    events += Bencode.DictionaryBegin()
    message match {
      case error: dht.message.Error =>
        // key 'e'
        events += Bencode.Character('e')
        // value (list)
        events += Bencode.ListBegin()
        events += Bencode.Integer(error.code)
        events += Bencode.JustString(error.message)
        events += Bencode.ListEnd()
      case query: dht.message.Query =>
        // key 'a'
        events += Bencode.Character('a')
        // value (dictionary)
        events += Bencode.DictionaryBegin()
        events += Bencode.JustString("id") += Bencode.Bytestring(query.id.toArray)
        query match {
          case fn: Query.FindNode =>
            events += Bencode.JustString("target") += Bencode.Bytestring(fn.target.toArray)
          case gp: Query.GetPeers =>
            events += Bencode.JustString("info_hash") += Bencode.Bytestring(gp.infohash.toArray)
          case ap: Query.AnnouncePeer =>
            events += Bencode.JustString("implied_port") += Bencode.Integer(if (ap.implied) 1 else 0)
            events += Bencode.JustString("info_hash") += Bencode.Bytestring(ap.infohash.toArray)
            events += Bencode.JustString("port") += Bencode.Integer(ap.port)
            events += Bencode.JustString("token") += Bencode.Bytestring(ap.token)
          case _ =>
        }
        events += Bencode.DictionaryEnd()
        events += Bencode.Character('q') += Bencode.JustString(query.name)
      case response: dht.message.Response =>
        // key 'r'
        events += Bencode.Character('r')
        // value (dictionary)
        events += Bencode.DictionaryBegin()
        events += Bencode.JustString("id") += Bencode.Bytestring(response.id.toArray)
        response match {
          case fn: Response.FindNode =>
            events += Bencode.JustString("nodes")
            events += Bencode.Bytestring(fn.nodes.foldLeft[Array[Byte]](Array.empty) { (array: Array[Byte], node: NodeInfo) =>
              array ++ node.id.toArray ++ Endpoint.isa2ba(node.address)
            })
          case gpn: Response.GetPeersWithNodes =>
            events += Bencode.JustString("nodes")
            events += Bencode.Bytestring(gpn.nodes.foldLeft[Array[Byte]](Array.empty) { (array: Array[Byte], node: NodeInfo) =>
              array ++ node.id.toArray ++ Endpoint.isa2ba(node.address)
            })
            events += Bencode.JustString("token") += Bencode.Bytestring(gpn.token)
          case gpv: Response.GetPeersWithValues =>
            events += Bencode.JustString("token") += Bencode.Bytestring(gpv.token)
            events += Bencode.JustString("values")
            events += Bencode.ListBegin()
            gpv.values foreach { peer => events += Bencode.Bytestring(Endpoint.isa2ba(peer)) }
            events += Bencode.ListEnd()
          case _ =>
        }
        events += Bencode.DictionaryEnd()
      case plugin: dht.message.Plugin =>
        events += Bencode.Character('p')
        events += Bencode.ListBegin()
        events += Bencode.Bytestring(plugin.id.toArray)
        events += Bencode.Integer(plugin.pid.value)
        events += Bencode.Bytestring(plugin.payload.toArray)
        events += Bencode.ListEnd()
    }
    events += Bencode.Character('t') += Bencode.Bytestring(message.tid.toArray)
    events += Bencode.Character('y') += Bencode.Character(message.y)
    events += Bencode.DictionaryEnd()

    ByteString(Bencode.encode(events):_*)
  }

}


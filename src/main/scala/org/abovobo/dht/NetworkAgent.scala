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

import akka.io.{Udp, IO}
import akka.actor.{ActorLogging, Cancellable, Actor}
import java.net.InetSocketAddress
import org.abovobo.integer.Integer160
import akka.util.ByteString
import akka.util.ByteStringBuilder
import org.abovobo.conversions.Bencode
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
 * This actor is responsible for sending Kademlia UDP messages and receiving them.
 * It also manages Kademlia transactions: when some sender initiates a query, this actor
 * will keep record about that fact and if remote party failed to respond in timely
 * manner, this actor will produce [[org.abovobo.dht.Controller.Fail]] command.
 *
 * @param endpoint An endpoint at which this agent must listen.
 * @param timeout  A period in time during which the remote party must respond to a query.
 */
class NetworkAgent(val endpoint: InetSocketAddress, val timeout: FiniteDuration) extends Actor with ActorLogging {

  import this.context.system

  /**
   * @inheritdoc
   *
   * Sends [[akka.io.Udp.Bind]] message to IO manager.
   */
  override def preStart() = {
    // initialize UDP bind procedure
    IO(Udp) ! Udp.Bind(self, this.endpoint)
  }

  /**
   * @inheritdoc
   *
   * Sends [[akka.io.Udp.Unbind]] message to IO manager.
   */
  override def postStop() = {
    // unbind UDP socket listener
    IO(Udp) ! Udp.Unbind
  }

  /**
   * @inheritdoc
   *
   * Actually calls `become(this.ready)` as soon as [[akka.io.Udp.Bound]] message received.
   */
  override def receive = {
    case Udp.Bound(local) =>
      this.log.info("Bound with local address {}", local)
      this.context.become(this.ready)
  }

  /**
   * Implements Send/Receive logic. Decodes network package into a [[org.abovobo.dht.Message]]
   * upon receival and if received message is [[org.abovobo.dht.Response]] checks if it corresponds
   * to existing transaction.
   */
  def ready: Actor.Receive = {

    // received a packet from UDP socket
    case Udp.Received(data, remote) => try {
      // parse received ByteString into a message instance
      val message = this.parse(data)
      // check if message completes pending transaction
      message match {
        case response: Response => this.transactions.remove(response.tid).foreach { _._2.cancel() }
        case _ => // do nothing
      }
      // forward received message to controller
      this.controller ! Controller.Receive(message)
    } catch {
      case e: NetworkAgent.ParsingException =>
        sender ! e.error
    }

    // `Send` command received
    case NetworkAgent.Send(message, remote) =>
      // if we are sending query - set up transaction monitor
      message match {
        case query: Query =>
          this.transactions.put(
            query.tid,
            query -> system.scheduler.scheduleOnce(this.timeout)(this.fail(query))(system.dispatcher))
        case _ => // do nothing
      }
      // send serialized message to remote address
      IO(Udp) ! Udp.Send(NetworkAgent.serialize(message), remote)
  }

  /**
   * Implements the case when remote peer has failed to respond to our query
   * in timely manner. In this case we generate [[org.abovobo.dht.Controller.Fail]]
   * command to controller actor.
   *
   * @param query A query which remote party failed to respond to in timely manner.
   */
  private def fail(query: Query) = {
    this.transactions.remove(query.tid).foreach { _._2.cancel() }
    this.controller ! Controller.Fail(query)
  }

  /**
   * Parses given [[akka.util.ByteString]] producing instance of corresponding
   * [[org.abovobo.dht.Message]] type. If something goes wrong, throws
   * [[org.abovobo.dht.NetworkAgent.ParsingException]] with corresponding
   * [[org.abovobo.dht.Error]] message which can be sent to remote party.
   *
   * @param data [[akka.util.ByteString]] instance representing UDP packet received
   *             from remote peer.
   * @return     [[org.abovobo.dht.Message]] instance of proper type.
   */
  private def parse(data: ByteString): Message = {

    import Endpoint._

    val dump = Bencode.decode(data).toIndexedSeq
    val n = dump.length

    val tid = dump(n - 4) match {
      case Bencode.Bytestring(value) => new TID(value)
      case _ => throw new IllegalArgumentException("Failed to retrieve transaction id")
    }

    def xthrow(code: Int, message: String) = throw new NetworkAgent.ParsingException(new Error(tid, code, message))

    def array(event: Bencode.Event): Array[Byte] = event match {
      case Bencode.Bytestring(value) => value
      case _ => xthrow(203, "Malformed packet")
    }

    def integer160(event: Bencode.Event): Integer160 = event match {
      case Bencode.Bytestring(value) => new Integer160(value)
      case _ => xthrow(203, "Malformed packet")
    }

    def string(event: Bencode.Event): String = event match {
      case Bencode.Bytestring(value) => new String(value, "UTF-8")
      case _ => xthrow(203, "Malformed packet")
    }

    def nodes(event: Bencode.Event): IndexedSeq[Node] = event match {
      case Bencode.Bytestring(value) =>
        val sz = Integer160.bytesize
        val n = value.length / (sz + Endpoint.IPV4_ADDR_SIZE + 2)
        for (i <- 0 until n) yield new Node(new Integer160(value.take(sz)), value.drop(sz))
      case _ => xthrow(203, "Malformed packet")
    }

    def peers(events: IndexedSeq[Bencode.Event]): IndexedSeq[Peer] = events map {
      case Bencode.Bytestring(value) => Endpoint.ba2isa(value)
      case _ => xthrow(203, "Malformed packet")
    }

    def integer(event: Bencode.Event): Long = event match {
      case Bencode.Integer(value) => value
      case _ => xthrow(203, "Malformed packet")
    }

    dump(n - 2) match {
      case Bencode.Bytestring(value) => value(0) match {
        case 'e' =>
          new Error(tid, integer(dump(n - 8)), string(dump(n - 7)))
        case 'q' =>
          string(dump(n - 6)) match {
            case "ping" =>
              new Query.Ping(tid, integer160(dump(n - 9)))
            case "find_node" =>
              new Query.FindNode(tid, integer160(dump(n - 11)), integer160(dump(n - 9)))
            case "get_peers" =>
              new Query.GetPeers(tid, integer160(dump(n - 11)), integer160(dump(n - 9)))
            case "announce_peer" =>
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
            case _ => xthrow(204, "Unknown query")
          }
        case 'r' =>
          if (this.transactions.get(tid).isDefined) {
            this.transactions(tid)._1 match {
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
                  case _ => xthrow(203, "Malformed packet")
                }
              case ap: Query.AnnouncePeer =>
                new Response.AnnouncePeer(tid, integer160(dump(n - 7)))
              case _ => xthrow(201, "Unknown corresponding query type")
            }
          } else {
            xthrow(203, "Invalid transaction id")
          }
      }
      case _ => xthrow(204, "Unknown method")
    }
  }

  /// Instantiates a map of associations between transaction identifiers
  /// and cancellable tasks which will produce failure command if remote peer
  /// failed to respond in timely manner.
  private val transactions = new mutable.HashMap[TID, (Query, Cancellable)]

  /// Initializes sibling `controller` actor reference
  private lazy val controller = this.context.actorSelection("../controller")
}

/** Accompanying object */
object NetworkAgent {

  /** Base trait for all commands natively supported by [[org.abovobo.dht.NetworkAgent]] actor. */
  sealed trait Command

  /**
   * Command instructing actor to send given message to given remote peer.
   *
   * @param message A message to send.
   * @param remote  An address (IP/Port) to send message to.
   */
  case class Send(message: Message, remote: InetSocketAddress) extends Command

  /**
   * Represents exception which may happen during packet parsing process.
   * Note that this exception will normally result in sending [[org.abovobo.dht.Error]]
   * message back to remote peer which sent us a packet. It means, that effectively
   * this exception will only be thrown after bencoded packet already decoded successfully
   * and transaction identifier located correctly.
   *
   * @param error An error message to send back to remote peer.
   */
  private class ParsingException(val error: Error) extends Exception

  /**
   * Serializes given message into a packet sendable via network.
   *
   * @param message A message to serialize
   * @return [[akka.util.ByteString]] instance which can be sent via network.
   */
  private def serialize(message: Message): ByteString = {

    val buf = new ByteStringBuilder()
    buf += 'd'

      message match {
        case error: Error =>
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
              val nodes = r.nodes.foldLeft[Array[Byte]](Array.empty) { (array: Array[Byte], node: Node) =>
                array ++ node.id.toArray ++ Endpoint.isa2ba(node.address)
              }
              buf ++= nodes.length.toString.getBytes("UTF-8") += ':' ++= nodes
            case r: Response.GetPeersWithNodes =>
              // "nodes" -> response.nodes
              buf += '5' += ':' ++= "nodes".getBytes("UTF-8")
              val nodes = r.nodes.foldLeft[Array[Byte]](Array.empty) { (array: Array[Byte], node: Node) =>
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
          }
          buf += 'e'
      }
      buf += '1' += ':' += 't'
      buf ++= message.tid.value.length.toString.getBytes("UTF-8") += ':' ++= message.tid.value
      buf += '1' += ':' += 'y'
      buf += '1' += ':' += message.y

    buf += 'e'
    buf.result()
  }
}

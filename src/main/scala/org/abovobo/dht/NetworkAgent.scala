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

  override def preStart() = {
    // initialize UDP bind procedure
    IO(Udp) ! Udp.Bind(self, this.endpoint)
  }

  override def postStop() = {
    // unbind UDP socket listener
    IO(Udp) ! Udp.Unbind
  }

  override def receive = {
    case Udp.Bound(local) =>
      this.log.info("Bound with local address {}", local)
      this.context.become(this.ready)
  }

  def ready: Actor.Receive = {

    // received a packet from UDP socket
    case Udp.Received(data, remote) =>
      // parse received ByteString into a message instance
      val message = this.parse(data)
      // check if message completes pending transaction
      message match {
        case response: Response =>
          this.transactions.remove(response.tid).foreach { _._2.cancel() }
        case _ => // do nothing
      }
      // forward received message to controller
      this.controller ! Controller.Receive(message)

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

  private def parse(data: ByteString): Message = {

    import Endpoint._

    def xthrow = throw new IllegalArgumentException("Invalid message")

    def array(event: Bencode.Event): Array[Byte] = event match {
      case Bencode.Bytestring(value) => value
      case _ => xthrow
    }

    def integer160(event: Bencode.Event): Integer160 = event match {
      case Bencode.Bytestring(value) => new Integer160(value)
      case _ => xthrow
    }

    def string(event: Bencode.Event): String = event match {
      case Bencode.Bytestring(value) => new String(value, "UTF-8")
      case _ => xthrow
    }

    def nodes(event: Bencode.Event): IndexedSeq[Node] = event match {
      case Bencode.Bytestring(value) =>
        val sz = Integer160.bytesize
        val n = value.length / (sz + Endpoint.IPV4_ADDR_SIZE + 2)
        for (i <- 0 until n) yield new Node(new Integer160(value.take(sz)), value.drop(sz))
      case _ => xthrow
    }

    def peers(events: IndexedSeq[Bencode.Event]): IndexedSeq[Peer] = events map {
      case Bencode.Bytestring(value) => Endpoint.ba2isa(value)
      case _ => xthrow // todo error 202
    }

    def integer(event: Bencode.Event): Long = event match {
      case Bencode.Integer(value) => value
      case _ => xthrow
    }

    val dump = Bencode.decode(data).toIndexedSeq
    val n = dump.length

    val tid = dump(n - 4) match {
      case Bencode.Bytestring(value) => new TID(value)
      case _ => xthrow
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
            case _ => xthrow // todo Produce Error message 204 and send it back to remote peer
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
                  case _ => xthrow // todo error 202
                }
              case ap: Query.AnnouncePeer =>
                new Response.AnnouncePeer(tid, integer160(dump(n - 7)))
              case _ => xthrow // todo Produce Error message 202
            }
          } else {
            xthrow // todo Produce Error message 203 `invalid transaction` and send it back to remote peer
          }
      }
      case _ => xthrow
    }
  }

  /// Instantiates a map of associations between transaction identifiers
  /// and cancellable tasks which will produce failure command if remote peer
  /// failed to respond in timely manner.
  private val transactions = new mutable.HashMap[TID, (Query, Cancellable)]

  /// Initializes sibling `controller` actor reference
  private lazy val controller = this.context.actorSelection("../controller")
}

object NetworkAgent {

  sealed trait Command

  case class Send(message: Message, remote: InetSocketAddress) extends Command

  /*
  sealed trait Message
  sealed trait Query extends Message
  sealed trait Response extends Message

  case class Error(remote: InetSocketAddress, tid: TID, code: Int, message: String)

  object Query {

    case class         Ping(remote: InetSocketAddress,
                            tid: TID,
                            id: Integer160) extends Query

    case class     FindNode(remote: InetSocketAddress,
                            tid: TID,
                            id: Integer160,
                            target: Integer160) extends Query

    case class     GetPeers(remote: InetSocketAddress,
                            tid: TID,
                            id: Integer160,
                            infohash: Integer160) extends Query

    case class AnnouncePeer(remote: InetSocketAddress,
                            tid: TID,
                            id: Integer160,
                            infohash: Integer160,
                            token: Array[Byte],
                            port: Int,
                            implied: Boolean) extends Query
  }

  object Response {

    case class PingOrAnnouncePeer(remote: InetSocketAddress,
                                  tid: TID,
                                  id: Integer160) extends Response

    case class           FindNode(remote: InetSocketAddress,
                                  tid: TID,
                                  id: Integer160,
                                  nodes: Array[InetSocketAddress]) extends Response

    case class GetPeersWithValues(remote: InetSocketAddress,
                                  tid: TID,
                                  id: Integer160,
                                  token: Array[Byte],
                                  peers: Array[InetSocketAddress]) extends Response

    case class  GetPeersWithNodes(remote: InetSocketAddress,
                                  tid: TID,
                                  id: Integer160,
                                  token: Array[Byte],
                                  nodes: Array[InetSocketAddress]) extends Response
  }

  class MessageBuilder {
    type Dictionary = TreeMap[Bencode.Bytestring, Bencode.Event]
    var root: Dictionary = null
    var current: Dictionary = null
    def startDictionary =
    if (this.current == null) current = root
    else {
      this.root +=
    }
    def endDictionary
    //def set(b: Byte)
  }
*/

  private def serialize(message: Message): ByteString = {
    null
  }

    /*
    val stack = new mutable.Stack[State.Value]
    stack.push(State.Begin)

    while (iterator.hasNext) {
      val event = iterator.next()
      stack.top match {
        case State.Begin => event match {
          case Bencode.DictionaryBegin =>
            stack.push(State.Dictionary)
          case _ => xthrow()
        }
        case State.Dictionary => event match {
          case Bencode.Bytestring(value) =>
            if (value.length == 1)
              if (value(0) == 'a') stack.push(State.Query)
              else if (value(0) == 'r') stack.push(State.Response)
              else if (value(0) == 'e') stack.push(State.Error)
              else xthrow()
            else xthrow()
          case _ => xthrow()
        }
        case State.Query => event match {

        }
      }
    }
    */
    //null
}

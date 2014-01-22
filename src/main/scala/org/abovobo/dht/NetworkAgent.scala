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
import akka.actor.Actor
import java.net.InetSocketAddress
import org.abovobo.integer.Integer160
import akka.util.ByteString
import org.abovobo.conversions.Bencode
import scala.collection.mutable

/**
 * This actor is responsible for sending Kademlia UDP messages and receiving them.
 * It also manages Kademlia transactions: when some sender initiates a query, this actor
 * will keep record about that fact and when response message is received, it will be
 * delivered back to initiator.
 */
class NetworkAgent extends Actor {

  import this.context.system

  override def preStart() = {
    // initialize UDP bind procedure
    IO(Udp) ! Udp.Bind(self, new InetSocketAddress(0))
  }

  override def postStop() = {
    // unbind UDP socket listener
    IO(Udp) ! Udp.Unbind
  }

  override def receive = {
    case Udp.Bound(local) => //
    case Udp.Received(data, remote) => // TODO Implement message receiving
      this.controller ! NetworkAgent.parse(data, remote)
    case
    //case Udp.Unbind => socket ! Udp.Unbind
    //case Udp.Unbound => this.context.unbecome()
  }

  /// Initializes sibling `controller` actor reference
  private lazy val controller = this.context.actorSelection("../controller")
}

object NetworkAgent {

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
    def set(b: Byte)
  }

  private def parse(data: ByteString, remote: InetSocketAddress): Message = {
    object State extends Enumeration {
      val Begin, Dictionary, Query, Response, Error, List = Value
    }

    def xthrow() = throw new IllegalArgumentException("Invalid message")
    val iterator = Bencode.decode(data)
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
    null
  }
}

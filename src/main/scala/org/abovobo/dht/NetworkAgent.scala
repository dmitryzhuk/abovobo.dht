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
    //case Udp.Unbind => socket ! Udp.Unbind
    //case Udp.Unbound => this.context.unbecome()
  }

}

object NetworkAgent {

  sealed trait Message
  sealed trait Query extends Message
  sealed trait Response extends Message

  case class Error(remote: InetSocketAddress, tid: TID, code: Int, message: String)

  object Query {
    case class Ping(remote: InetSocketAddress, tid: TID, id: Integer160) extends Query
    case class FindNode(remote: InetSocketAddress, tid: TID, id: Integer160, target: Integer160) extends Query
    case class GetPeers(remote: InetSocketAddress, tid: TID, id: Integer160, infohash: Integer160) extends Query
  }

  object Response {
    case class Ping(tid: TID, id: Integer160) extends Response
    case class FindNode(tid: TID, id: Integer160, nodes: Array[InetSocketAddress]) extends Response
    trait GetPeers extends Response
    case class GetPeersWithValues(tid: TID, id: Integer160, token: Array[Byte], peers: Array[InetSocketAddress]) extends GetPeers
    case class GetPeersWithNodes(tid: TID, id: Integer160, token: Array[Byte], nodes: Array[InetSocketAddress]) extends GetPeers
  }
}

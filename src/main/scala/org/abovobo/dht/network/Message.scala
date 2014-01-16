/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.network

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

sealed trait Message

object Query {
  case class Ping() extends Message
  case class FindNode() extends Message
  case class AnnouncePeer() extends Message
  case class GetPeers() extends Message
}

object Response {
  case class Ping() extends Message
  case class FindNode() extends Message
  case class AnnouncePeer() extends Message
  case class GetPeers() extends Message
}

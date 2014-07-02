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

import akka.util.ByteString
import org.abovobo.dht
import org.abovobo.dht.{message, TID}
import org.abovobo.integer.Integer160

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

}


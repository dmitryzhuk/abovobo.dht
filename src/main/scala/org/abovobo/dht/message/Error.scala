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

import org.abovobo.dht
import org.abovobo.dht.TID


/**
 * Concrete [[dht.message.Message]] implementation, representing error.
 *
 * @param tid     Transaction identifier.
 * @param code    Error code.
 * @param message Error message.
 */
class Error(tid: TID, val code: Long, val message: String) extends Message(tid, 'e') {

  /** @inheritdoc */
  override def toString = "E: " + code + ". " + message + " [tid: " + tid + "]"

  /** @inheritdoc */
  def toBencodedString =
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

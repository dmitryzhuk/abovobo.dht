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
import org.abovobo.dht.TID
import org.abovobo.dht.PID
import org.abovobo.integer.Integer160

/**
 * This class represents basic abstract class for all DHT plugin messages.
 *
 * @param tid     Transaction identifier.
 * @param id      Sending node identifier.
 * @param pid     Plugin identifier.
 * @param payload Useful content of the message.
 */
abstract class Plugin(tid: TID, id: Integer160, val pid: PID, val payload: ByteString)
  extends Normal(tid, 'p', id) {

  /** @inheritdoc */
  override def toString = "plugin#" + pid + ": tid: " + tid + ", self: " + id

  // TODO Complete implementation
  /** @inheritdoc */
  override def toBencodedString = "@IMPLEMENTME"
}

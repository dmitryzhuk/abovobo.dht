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

import org.abovobo.dht.TID
import org.abovobo.integer.Integer160

/**
 * Abstract class representing normal (in opposite to error) message.
 *
 * @param tid Transaction identifier.
 * @param y   Message kind: 'e' for error, 'q' for query, 'r' for response.
 * @param id  Sending node identifier.
 */
abstract class Normal(tid: TID, y: Char, val id: Integer160) extends Message(tid, y)

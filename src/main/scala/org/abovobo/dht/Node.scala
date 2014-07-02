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

import org.abovobo.integer.Integer160
import java.net.InetSocketAddress
import java.util.Date
import scala.concurrent.duration._

/**
 * This class represents remote DHT node descriptor.
 *
 * @constructor     Creates new instance of Node.
 *
 * @param id        SHA-1 identifier of the node.
 * @param address   Network address of remote node.
 */
class Node(val id: Integer160, val address: InetSocketAddress) {
  override def toString = "Node [#" + id + "@" + address.getAddress.getHostAddress + ":" + address.getPort + "]" 
}

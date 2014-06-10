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

/**
 * This class represents complete Node information which is kept in persistent storage.
 *
 * @constructor     Creates new instance of PersistentNode.
 *
 * @param id        SHA-1 identifier of the node.
 * @param address   Node network address.
 * @param bucket    SHA-1 identifier of the lower bound of the bucket this node belongs to.
 * @param replied   A time when this node last replied to our query.
 * @param queried   A time when this node last sent us a query.
 * @param failcount A number of times in a row this node failed to respond to our query.
 *                  This value reset to zero when node has replied to our query.
 *
 * @author Dmitry Zhuk
 */
class PersistentNode(id: Integer160,
                     address: InetSocketAddress,
                     val bucket: Integer160,
                     val replied: Option[Date],
                     val queried: Option[Date],
                     val failcount: Int) extends Node(id, address) {

  /**
   * Returns true if this node is "good".
   *
   * @param timeout   Duration of period of inactivity after that node becomes questionnable.
   * @param threshold Number of fails to reply before node becomes bad.
   * @return  true if this node is considered to be good.
   */
  def good(implicit timeout: Duration, threshold: Int): Boolean = !this.bad && !this.questionnable

  /**
   * Returns true if this node is definitely bad
   *
   * @param timeout   Duration of period of inactivity after that node becomes questionnable.
   * @param threshold Number of fails to reply before node becomes bad.
   * @return  true if this node is considered to be good.
   */
  def bad(implicit timeout: Duration, threshold: Int): Boolean = threshold <= this.failcount

  /**
   * Returns true if this node is not "bad" but has not been recently seen
   *
   * @param timeout   Duration of period of inactivity after that node becomes questionnable.
   * @param threshold Number of fails to reply before node becomes bad.
   * @return  true if this node is considered to be good.
   */
  def questionnable(implicit timeout: Duration, threshold: Int): Boolean =
    threshold > this.failcount && this.replied.isDefined && !this.recentlySeen

  /**
   * Returns true if this node has been seen recently
   *
   * @param timeout   Duration of period of inactivity after that node becomes questionnable.
   * @param threshold Number of fails to reply before node becomes bad.
   * @return  true if this node is considered to be good.
   */
  def recentlySeen(implicit timeout: Duration, threshold: Int): Boolean =
    this.replied.map(d => (System.currentTimeMillis - d.getTime).milliseconds).getOrElse(Duration.Inf) < timeout ||
      this.queried.map(d => (System.currentTimeMillis - d.getTime).milliseconds).getOrElse(Duration.Inf) < timeout
}

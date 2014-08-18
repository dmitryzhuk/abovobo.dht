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

import java.net.InetSocketAddress
import java.util.Date

import org.abovobo.integer.Integer160

import scala.concurrent.duration._


/**
 * This class represents complete NodeInfo information which is known to the system after at least single
 * attempt to communicate with that NodeInfo.
 *
 * @constructor     Creates new instance of KnownNodeInfo.
 *
 * @param id        SHA-1 identifier of the node.
 * @param address   NodeInfo network address.
 * @param bucket    SHA-1 identifier of the lower bound of the bucket this node belongs to.
 * @param replied   A time when this node last replied to our query.
 * @param queried   A time when this node last sent us a query.
 * @param failcount A number of times in a row this node failed to respond to our query.
 *                  This value reset to zero when node has replied to our query.
 *
 * @author Dmitry Zhuk
 */
class KnownNodeInfo(id: Integer160,
                address: InetSocketAddress,
                val replied: Option[Date],
                val queried: Option[Date],
                val failcount: Int) extends NodeInfo(id, address) {

  /// Ensure that we are dealing with "real" node
  require(id != Integer160.zero)

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

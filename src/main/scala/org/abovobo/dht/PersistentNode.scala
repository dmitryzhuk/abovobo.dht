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

import java.util.Date
import org.abovobo.integer.Integer160
import scala.concurrent.duration._

/**
 * This class extends Node adding features for managing the node within a [[org.abovobo.dht.RoutingTable]].
 *
 * @constructor Creates new instance of PersistentNode.
 *
 * @param bucket    SHA-1 identifier of the min bound of the bucket this node belongs to.
 * @param replied   A time when this node last replied to our query.
 * @param queried   A time when this node last sent us a query.
 * @param failcount A number of times in a row this node failed to respond to our query.
 *                  This value reset to zero when node has replied to our query.
 * @param timeout   Time interval before node becomes questionable.
 * @param threshold Number of times a node must fail to respond to become 'bad'.
 */
class PersistentNode(id: Integer160,
                     ipv4u: Option[Endpoint],
                     ipv4t: Option[Endpoint],
                     ipv6u: Option[Endpoint],
                     ipv6t: Option[Endpoint],
                     val bucket: Integer160,
                     val replied: Option[Date],
                     val queried: Option[Date],
                     val failcount: Int,
                     val timeout: Duration,
                     val threshold: Int)
  extends Node(id, ipv4u, ipv4t, ipv6u, ipv6t) {

  /** Returns true if this node is "good" */
  def good: Boolean = !this.bad && !this.questionnable

  /** Returns true if this node is definitely bad */
  def bad: Boolean = this.threshold <= this.failcount

  /** Returns true if this node is not "bad" but has not been recently seen */
  def questionnable: Boolean = this.threshold > this.failcount && this.replied.isDefined && !this.recentlySeen

  /** Returns true if this node has been seen recently */
  def recentlySeen: Boolean =
    this.replied.map(d => (System.currentTimeMillis - d.getTime).milliseconds).getOrElse(Duration.Inf) < timeout ||
      this.queried.map(d => (System.currentTimeMillis - d.getTime).milliseconds).getOrElse(Duration.Inf) < timeout

  /*
    (this.replied.isDefined && ((System.currentTimeMillis - this.replied.get.getTime).milliseconds < timeout)) ||
      (this.queried.isDefined && ((System.currentTimeMillis - this.queried.get.getTime).milliseconds < timeout))
      */
}

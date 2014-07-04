/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.controller

import org.abovobo.integer.Integer160
import org.abovobo.dht.Node

/**
 * Defines simple class holding request round information. Round is represented by the collection
 * of requests and has a single method returning actual round result.
 *
 * @param requests A collection of requests owned by this round.
 */
private[controller] class Round(val requests: Traversable[Request]) {

  /**
   * Instantiates new Round using collection [[Node]] objects.
   *
   * @param nodes A collection of [[Node]] objects to create round of requests to.
   */
  def this(nodes: Seq[Node]) = this(nodes.map(new Request(_)))

  /**
   * Returns [[Request.Unknown]] if there is at least one request has this result.
   * If all requests has known result, returns [[Request.Failed]] if all requests
   * were failed, [[Request.Improved(n)]] if at least one request improved the collection
   * of seen nodes or [[Request.Neutral]] otherwise.
   *
   * @return result of whole round of requests
   */
  def result = {
    var unknown = 0
    var improved = 0
    var failed = 0
    this.requests foreach {
      _.result match {
        case Request.Improved(n) => improved += n
        case Request.Unknown() => unknown += 1
        case Request.Failed() => failed += 1
        case Request.Neutral() => // do nothing
      }
    }
    if (unknown > 0) Request.Unknown()
    else if (improved > 0) Request.Improved(improved)
    else if (failed == requests.size) Request.Failed()
    else Request.Neutral()
  }

  /**
   * Returns value indicating number of improvements which this Round made
   * to a collection of known nodes so far.
   */
  def improved = this.requests.foldLeft(0) { (i, r) =>
    r.result match {
      case Request.Improved(n) => i + n
      case _ => i
    }
  }

  /**
   * Returns [[Some]] [[Request]] to given [[Node]] or [[Node]] if not found.
   *
   * @param node An instance of [[Node]] to get request for.
   * @return Request to given node or [[None]].
   */
  def get(node: Node): Option[Request] =
    if (node.id == Integer160.zero)
      // For node with zero id (router) test both id and address
      this.requests.find(r => r.node.id == node.id && r.node.address.equals(node.address))
    else
      // For normal node test id only
      this.requests.find(_.node.id == node.id)
}

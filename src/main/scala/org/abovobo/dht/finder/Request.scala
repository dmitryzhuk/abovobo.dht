/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.finder

import org.abovobo.dht.NodeInfo

/**
 * Defines simple class holding request information used by [[Finder]] class.
 *
 * @param node   A node which is being requested
 * @param result A result of request
 */
private[finder] class Request(val node: NodeInfo, var result: Request.Result = Request.Unknown())

/**
 * Accompanying object holding request result enumeration definition
 */
private[finder] object Request {

  /** Basic trait for all possible result cases */
  sealed trait Result

  /** Result is still unknown, request is in progress */
  case class Unknown() extends Result

  /**
   * Request has improved the collection of known nodes by `n` nodes which are
   * closer than previously seen.
   *
   * @param n Number of nodes reported, which are closer than previously seen.
   */
  case class Improved(n: Int) extends Result

  /** Request was successful, but did not improve the collection of known nodes. */
  case class Neutral() extends Result

  /** Request has failed probably due to timeout */
  case class Failed() extends Result

}

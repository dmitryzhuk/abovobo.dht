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

/**
 * Defines simple class holding request round information. Round is represented by the collection
 * of requests and has a single method returning actual round result.
 *
 * @param requests A collection of requests owned by this round.
 */
private[controller] class Round(val requests: Traversable[Request]) {

  /**
   * Returns [[Request.Result.Unknown]] if there is at least one request has this result.
   * If all requests has known result, returns [[Request.Result.Failed]] if all requests
   * were failed, [[Request.Result.Improved]] if at least one request improved the collection
   * of seen nodes or [[Request.Result.Neutral]] otherwise.
   *
   * @return result of whole round of requests
   */
  def result = {
    var unknown = 0
    var improved = 0
    var failed = 0
    requests foreach {
      _.result match {
        case Request.Result.Improved => improved += 1
        case Request.Result.Unknown => unknown += 1
        case Request.Result.Failed => failed += 1
        case Request.Result.Neutral =>
      }
    }
    if (unknown > 0) Request.Result.Unknown
    else if (improved > 0) Request.Result.Improved
    else if (failed == requests.size) Request.Result.Failed
    else Request.Result.Neutral
  }
}

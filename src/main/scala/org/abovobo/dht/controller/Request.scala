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

import org.abovobo.dht.Node

/**
 * Defines simple class holding request information used by [[Finder]] class.
 *
 * @param node   A node which is being requested
 * @param result A result of request
 */
private[controller] class Request(val node: Node, var result: Request.Result.Value = Request.Result.Unknown)

/**
 * Accompanying object holding request result enumeration definition
 */
private[controller] object Request {

  object Result extends Enumeration {
    val
    // Initial value indicating that request is still in progress
    Unknown,

    // Indicates that requested node brought information about new nodes
    // that are closer than already seen.
    Improved,

    // Indicates that requested node successfully responded but didn't bring new nodes or
    // there was no nodes closer than already seen reported.
    Neutral,

    // Indicates that requested node failed to respond.
    Failed

    = Value
  }

}

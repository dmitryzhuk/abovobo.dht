/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo

import java.net.InetSocketAddress

/** General package types */
package object dht {

  /** Peer information is just an internet address (IP/Port pair) of remote node */
  type Peer = InetSocketAddress

}

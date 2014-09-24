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

/**
 * This class represents a single entity of known peer stored in the storage.
 *
 * @param infohash  An infohash which was announced by the peer
 * @param address   A network address of the peer
 * @param announced Timestamp when this announcement has been made
 */
class KnownPeerInfo(val infohash: Integer160, val address: InetSocketAddress, val announced: Date)

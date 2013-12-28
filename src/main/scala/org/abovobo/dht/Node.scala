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

/**
 * This class represents DHT node descriptor.
 *
 * @constructor Creates new instance of Node.
 *
 * @param id        SHA-1 identifier of the node.
 * @param ipv4u     Optional UDP IPv4 endpoint of the node.
 * @param ipv4t     Optional TCP IPv4 endpoint of the node.
 * @param ipv6u     Optional UDP IPv6 endpoint of the node.
 * @param ipv6t     Optional TCP IPv6 endpoint of the node.
 */
class Node(val id: Integer160,
           val ipv4u: Option[Endpoint],
           val ipv4t: Option[Endpoint],
           val ipv6u: Option[Endpoint],
           val ipv6t: Option[Endpoint])


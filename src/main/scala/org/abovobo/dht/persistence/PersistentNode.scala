/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.persistence

import java.net.InetSocketAddress
import java.util.Date
import org.abovobo.integer.Integer160

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
class PersistentNode(val id: Integer160,
                     val address: InetSocketAddress,
                     val bucket: Integer160,
                     val replied: Option[Date],
                     val queried: Option[Date],
                     val failcount: Int)

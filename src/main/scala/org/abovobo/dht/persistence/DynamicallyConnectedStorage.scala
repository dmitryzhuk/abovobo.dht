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

/**
 * Created by dmitryzhuk on 8/15/14.
 */
class DynamicallyConnectedStorage(val ds: DataSource) extends AutoCloseable {

  override def close() = Unit

}

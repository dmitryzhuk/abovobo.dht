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
 * Defines base trait for data source (connection factory)
 */
trait DataSource extends AutoCloseable {

  /** Returns new connection */
  def connection: java.sql.Connection
}

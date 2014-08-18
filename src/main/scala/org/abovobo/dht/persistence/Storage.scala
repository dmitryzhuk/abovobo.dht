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
 * Basic storage trait definition.
 */
trait Storage extends AutoCloseable with Reader with Writer {

  /**
   * Returns connection to work with.
   *
   * @return connection to work with.
   */
  protected def connection: java.sql.Connection

  /**
   * Sets given schema as default for all storage operations.
   *
   * @param name A name of schema to set.
   */
  def setSchema(name: String): Unit

  /**
   * Unset default schema.
   */
  def unsetSchema(): Unit

  /**
   * Allows to execute block of code within transaction.
   *
   * @param f   A code block to execute.
   * @tparam T  Return type of the code block.
   * @return    A value yielded by code block.
   */
  def transaction[T](f: => T): T

}

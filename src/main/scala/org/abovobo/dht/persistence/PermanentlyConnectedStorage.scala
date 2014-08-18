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

import java.sql.{Connection, PreparedStatement}

import org.abovobo.jdbc.Transaction

/**
 * Represents abstract persistent storage which uses single connection throughout its lifecycle.
 * Note that the storage will close the connection in the end.
 *
 * @param connection A connection to be used by this storage.
 */
abstract class PermanentlyConnectedStorage(override protected val connection: Connection) extends Storage {

  /** @inheritdoc */
  override def close() = {
    this.statements.foreach(_._2.close())
    this.connection.close()
  }

  /** @inheritdoc */
  override def transaction[T](f: => T): T = Transaction.transaction(this.connection)(f)

  /** @inheritdoc */
  override protected def statement(key: String): java.sql.PreparedStatement = this.statements(key)

  /**
   * Prepares all statements which subclass wants to be closed automatically.
   *
   * @return A map of named prepared statements.
   */
  protected def prepare(): Map[String, PreparedStatement]

  /// Collection of named and prepared statements
  private val statements: Map[String, PreparedStatement] = this.prepare()
}



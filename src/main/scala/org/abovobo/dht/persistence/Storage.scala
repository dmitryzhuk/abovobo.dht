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

/**
 * Represetns abstract persistent storage.
 *
 * @author Dmitry Zhuk
 */
abstract class Storage(val connection: Connection) extends AutoCloseable {
  /**
   * @inheritdoc
   *
   * Really just closes all prepared statements and JDBC connection instance.
   */
  override def close() = {
    this.statements.foreach(_._2.close())
    this.connection.close()
  }

  /** Delegates invocation to JDBC [[java.sql.Connection]] instance */
  def commit() = this.connection.commit()

  /** Delegates invocation to JDBC [[java.sql.Connection]] instance */
  def rollback() = this.connection.rollback()

  /**
   * Prepares all statements which subclass wants to be closed automatically.
   *
   * @return A map of named prepared statements.
   */
  protected def prepare(): Map[String, PreparedStatement]

  /// An instance of connection
  //protected var connection: Connection = null

  /// Collection of named and prepared statements
  protected var statements: Map[String, PreparedStatement] = this.prepare()
}



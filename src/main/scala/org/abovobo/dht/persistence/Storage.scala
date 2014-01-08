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

import java.sql.{PreparedStatement, DriverManager, Connection}
import org.abovobo.jdbc.Transaction

/**
 * Represetns abstract persistent storage.
 *
 * @param driver  JDBC [[java.sql.DriverManager]] class name.
 * @param uri     JDBC connection string.
 *
 * @author Dmitry Zhuk
 */
abstract class Storage(val driver: String, val uri: String) extends AutoCloseable {

  /**
   * Loads actual [[java.sql.DriverManager]] class and gets connection to db,
   * then calls <code>prepare</code> method to collect statements.
   */
  def open(): Unit = {
    Class.forName(this.driver)
    this.connection = DriverManager.getConnection(this.uri)
    this.statements = this.prepare()
  }

  /**
   * @inheritdoc
   *
   * Really just closes all prepared connections and JDBC connection instance.
   */
  override def close() = {
    import org.abovobo.jdbc.Closer._
    this.statements.foreach(_._2.dispose())
    this.connection.dispose()
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
  protected var connection: Connection = null

  /// Collection of named and prepared statements
  protected var statements: Map[String, PreparedStatement] = null
}



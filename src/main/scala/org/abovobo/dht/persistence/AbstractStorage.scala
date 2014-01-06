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
 */
abstract class AbstractStorage(val driver: String, val uri: String) extends AutoCloseable {

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
   * Prepares all statements which subclass wants to be closed automatically.
   *
   * @return A map of named prepared statements.
   */
  protected def prepare(): Map[String, PreparedStatement]

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

  /**
   * Allows to execute block of code within transaction using [[org.abovobo.jdbc.Transaction]].
   *
   * @param f   A code block to execute.
   * @tparam T  Return type of the code block.
   * @return    A value yielded by code block.
   */
  def transaction[T](f: => T): T = Transaction.transaction(this.connection)(f)

  /// An instance of connection
  protected var connection: Connection = null

  /// Collection of named and prepared statements
  protected var statements: Map[String, PreparedStatement] = null
}

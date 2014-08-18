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

import org.abovobo.jdbc.Closer._
import org.abovobo.jdbc.Transaction

/**
 * Represents abstract persistent storage which takes new connection for every separate transaction
 * and closes it after transaction completion.
 *
 * @param ds  DataSource instance used to retrieve connection.
 */
abstract class DynamicallyConnectedStorage(val ds: DataSource) extends Storage {

  /// Optional schema name
  private var schema: Option[String] = None

  /// Currently active connection used while transaction is in progress
  private var current: Option[Connection] = None

  /**
   * Allows to invoke partially applied function of self for given SQL query.
   *
   * @param sql A query text to execute
   * @param f   Partially applied function to invoke.
   * @tparam T  Return type of invoked function.
   * @return    A result of invoked function.
   */
  protected def invoke[T](sql: String, f: PreparedStatement => T): T = using(this.connection) { c =>
    using(c.prepareStatement(sql)) { stmt =>
      f(stmt)
    }
  }

  /** @inheritdoc */
  override protected def connection = {
    this.current.getOrElse({
      val connection = this.ds.connection
      this.schema.foreach { name =>
        using(connection.createStatement()) { statement =>
          statement.execute("set schema " + name)
        }
      }
      connection.setAutoCommit(false)
      connection
    })
  }

  /** @inheritdoc */
  override def close() = Unit

  /** @inheritdoc */
  override def transaction[T](f: => T): T = using(this.connection) { connection =>
    val wrapped = new ConnectionWrapper(connection)
    this.current = Some(wrapped)
    val t: T = Transaction.transaction(connection)(f)
    wrapped.dispose()
    this.current = None
    t
  }

  /** @inheritdoc */
  override def setSchema(schema: String) = this.schema = Some(schema)

  /** @inheritdoc */
  override def unsetSchema() = this.schema = None
}

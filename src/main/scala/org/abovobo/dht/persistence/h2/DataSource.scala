/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.persistence.h2

import org.h2.jdbcx.JdbcConnectionPool
import org.h2.tools.RunScript

import org.abovobo.dht.persistence
import org.abovobo.jdbc.Closer._

object DataSource {

  /// Load JDBC drier
  Class.forName("org.h2.Driver")

  /** Value of the connection pool size by default */
  val DEFAULT_CONNECTION_POOL_SIZE = 10

  /**
   * Instantiates connection pool at given URL and wraps it with new instance of [[DataSource]].
   *
   * @param url   An URL to create connection pool at.
   * @param size  A size of the connection pool.
   * @return new instance of [[DataSource]]
   */
  def apply(url: String, size: Int): DataSource = {
    val cp = JdbcConnectionPool.create(url, "", "")
    cp.setMaxConnections(size)
    new DataSource(cp)
  }

  /**
   * Instantiates connection pool with default size at given URL and wraps it with new instance of [[DataSource]].
   *
   * @param url An URL to create connection pool at.
   * @return new instance of [[DataSource]]
   */
  def apply(url: String): DataSource = this.apply(url, this.DEFAULT_CONNECTION_POOL_SIZE)

  /**
   * Instantiates connection pool at given URL and wraps it with new instance of [[DataSource]]
   * and executes given script.
   *
   * @param url     An URL to create connection pool at.
   * @param script  A script to execute.
   * @param size    A size of the connection pool.
   * @return new instance of [[DataSource]]
   */
  def apply(url: String, script: java.io.Reader, size: Int = this.DEFAULT_CONNECTION_POOL_SIZE): DataSource = {
    val source = this.apply(url, size)
    val connection = source.connection
    try {
      RunScript.execute(connection, script)
      source
    } catch {
      case t: Throwable =>
        source.close()
        throw t
    } finally {
      connection.close()
    }
  }
}

/**
 * Represents wrapper over the [[javax.sql.DataSource]].
 *
 * @param ds An instance of [[javax.sql.DataSource]] to wrap.
 */
class DataSource(val ds: JdbcConnectionPool) extends persistence.DataSource {

  /** Returns new connection from given [[javax.sql.DataSource]] */
  override def connection = this.ds.getConnection

  /** Executes given script using H2 [[org.h2.tools.RunScript]] utility */
  override def execute(script: java.io.Reader): Unit = using(connection) { c =>
    RunScript.execute(c, script)
  }

  /** Calls [[org.h2.jdbcx.JdbcConnectionPool#dispose]] */
  override def close() = this.ds.dispose()

}

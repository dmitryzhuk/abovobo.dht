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

import java.sql.Connection

import org.abovobo.jdbc.Closer._
import org.h2.jdbcx.JdbcConnectionPool
import org.h2.tools.RunScript

import org.abovobo.dht.persistence

object DataSource {

  /// Load JDBC drier
  Class.forName("org.h2.Driver")

  /**
   * Instantiates connection pool at given URL and wraps it with new instance of [[DataSource]].
   *
   * @param url An URL to create connection pool at.
   * @return new instance of [[DataSource]]
   */
  def apply(url: String): DataSource = new DataSource(JdbcConnectionPool.create(url, "", ""))

  /**
   * Instantiates connection pool at given URL and wraps it with new instance of [[DataSource]]
   * and executes given script.
   *
   * @param url An URL to create connection pool at.
   * @param script A script to execute.
   * @return new instance of [[DataSource]]
   */
  def apply(url: String, script: java.io.Reader): DataSource = {
    val source = this.apply(url)
    using(source.connection) { connection: Connection =>
      RunScript.execute(connection, script)
    }
    source
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

  /** Calls [[org.h2.jdbcx.JdbcConnectionPool#dispose]] */
  override def close() = this.ds.dispose()

}

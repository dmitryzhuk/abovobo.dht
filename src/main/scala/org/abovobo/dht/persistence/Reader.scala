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

import org.abovobo.integer.Integer160
import java.util.Date
import java.sql.{ResultSet, PreparedStatement}
import scala.collection.mutable.ListBuffer
import org.abovobo.dht.Endpoint._
import org.abovobo.dht.{Peer, PersistentNode}

/**
 * This trait defines read-only methods accessing DHT persistent storage.
 *
 * Each access method presented by pair of public and protected methods.
 * Protected method receives prepared statement as an argument and performs
 * all necessary manipulations with it assuming documented layout of that
 * statement. Implementors will override public methods by redirecting execution
 * to protected methods supplying pre-instantiated [[java.sql.PreparedStatement]].
 *
 * @author Dmitry Zhuk
 */
trait Reader {

  /**
   * Returns DHT node SHA-1 identifier.
   *
   * @return DHT node SHA-1 identifier.
   */
  def id(): Option[Integer160]

  /**
   * Executes given [[java.sql.PreparedStatement]] to retrieve DHT node identifier.
   *
   * @param statement A statement to execute.
   * @return Some if there is an id in the storage, None otherwise.
   */
  protected def id(statement: PreparedStatement): Option[Integer160] = {
    import org.abovobo.jdbc.Closer._
    using(statement.executeQuery()) { rs =>
      if (rs.next()) Some(new Integer160(rs.getBytes("id"))) else None
    }
  }

  /**
   * Returns traversable collection of all persisted nodes.
   *
   * @return Traversable collection of all persisted nodes.
   */
  def nodes(): Traversable[PersistentNode]

  /**
   * Executes given query and reads all its rows converting them into instances of
   * [[org.abovobo.dht.PersistentNode]].
   *
   * @param statement A statement to execute.
   * @return Traversable collection of all persisted nodes.
   */
  protected def nodes(statement: PreparedStatement): Traversable[PersistentNode] = {
    import org.abovobo.jdbc.Closer._
    val nodes = new ListBuffer[PersistentNode]
    using(statement.executeQuery()) { rs =>
      while (rs.next()) {
        nodes += this.read(rs)
      }
    }
    nodes
  }

  /**
   * Returns persistent node with given id.
   *
   * @param id An SHA-1 identifier of the node to return.
   * @return Persistent node with given id.
   */
  def node(id: Integer160): Option[PersistentNode]

  /**
   * Sets statement parameters and executes query.
   *
   * Expected parameter mapping:
   * 1 BINARY id
   *
   * @param statement A statement to execute.
   * @param id        An SHA-1 identifier of the node to return.
   * @return Persistent node with given id.
   */
  protected def node(statement: PreparedStatement, id: Integer160): Option[PersistentNode] = {
    import org.abovobo.jdbc.Closer._
    statement.setBytes(1, id.toArray)
    using(statement.executeQuery()) { rs =>
      if (rs.next()) Some(this.read(rs)) else None
    }
  }

  /**
   * Returns traversable collection of all nodes within given bucket.
   *
   * @param id A lower bound of the bucket range.
   * @return Traversable collection of all nodes within given bucket.
   */
  def bucket(id: Integer160): Traversable[PersistentNode]

  /**
   * Sets statement parameters and executes query.
   *
   * Expected parameter mapping:
   * 1 BINARY id
   *
   * @param statement A statement to execute.
   * @param id        An SHA-1 identifier of the lower bound of the bucket to return nodes from.
   * @return Persistent node with given id.
   */
  protected def bucket(statement: PreparedStatement, id: Integer160): Traversable[PersistentNode] = {
    import org.abovobo.jdbc.Closer._
    val nodes = new ListBuffer[PersistentNode]
    statement.setBytes(1, id.toArray)
    using(statement.executeQuery()) { rs =>
      while (rs.next()) {
        nodes += this.read(rs)
      }
    }
    nodes
  }

  /**
   * Returns traversable collection of all buckets.
   *
   * @return Traversable collection of all buckets.
   */
  def buckets(): Traversable[(Integer160, Date)]

  /**
   * Executes given statement reading all rows into collection.
   *
   * @param statement A statement to execute.
   * @return Traversable collection of all buckets.
   */
  protected def buckets(statement: PreparedStatement): Traversable[(Integer160, Date)] = {
    import org.abovobo.jdbc.Closer._
    val buckets = new ListBuffer[(Integer160, Date)]
    using(statement.executeQuery()) { rs =>
      while (rs.next()) {
        buckets += new Integer160(rs.getBytes("id")) -> new Date(rs.getTimestamp("seen").getTime)
      }
    }
    buckets
  }

  /**
   * Returns an id of bucket following the given one.
   *
   * @param id A bucket to find next for.
   * @return   An id of bucket following the given one.
   */
  def next(id: Integer160): Integer160 = {
    val buckets = this.buckets().toIndexedSeq
    if (buckets.size == 0)
      Integer160.maxval
    else
      buckets.indexWhere(_._1 == id) match {
        case -1 => throw new IllegalArgumentException(id.toHexString)
        case n: Int if n < buckets.size - 1 => buckets(n + 1)._1
        case n: Int if n == buckets.size - 1 => Integer160.maxval
      }
  }

  /**
   * Returns traversable collection of peers associated with given infohash.
   *
   * @param infohash An infohash to get associated peers with.
   * @return traversable collection of peers associated with given infohash.
   */
  def peers(infohash: Integer160): Traversable[Peer]

  /**
   * Executes given statement reading all rows into a collection.
   *
   * Expected parameter mapping:
   * 1 BINARY infohash
   *
   * @param statement A statement to execute.
   * @param infohash  A parameter to set into a statement.
   * @return          Collection of peers.
   */
  protected def peers(statement: PreparedStatement, infohash: Integer160): Traversable[Peer] = {
    import org.abovobo.jdbc.Closer._
    val peers = new ListBuffer[Peer]
    statement.setBytes(1, infohash.toArray)
    using(statement.executeQuery()) { rs =>
      while (rs.next()) {
        peers += rs.getBytes("address")
      }
    }
    peers
  }

  /**
   * Reads instance of [[org.abovobo.dht.PersistentNode]] from given [[java.sql.ResultSet]].
   *
   * @param rs Valid [[java.sql.ResultSet]] instance.
   * @return New instance of [[org.abovobo.dht.PersistentNode]] built from read data.
   */
  private def read(rs: ResultSet): PersistentNode = {
    import org.abovobo.jdbc.Optional._
    new PersistentNode(
      new Integer160(rs.getBytes("id")),
      rs.getBytes("address"),
      new Integer160(rs.getBytes("bucket")),
      rs.timestamp("replied"),
      rs.timestamp("queried"),
      rs.getInt("failcount"))
  }
}

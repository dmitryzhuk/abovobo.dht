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

import java.sql.{PreparedStatement, ResultSet}
import java.util.Date

import org.abovobo.dht.Endpoint._
import org.abovobo.dht.{Bucket, KnownNodeInfo, Peer}
import org.abovobo.integer.Integer160
import org.abovobo.jdbc.Closer._

/**
 * This trait defines read-only methods accessing DHT persistent storage.
 *
 * Each access method presented by pair of public and protected method.
 * Protected method receives prepared statement as an argument and performs
 * all necessary manipulations with it assuming documented layout of that
 * statement. Implementers will override public methods by redirecting execution
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
   * Returns persistent node with given id.
   *
   * @param id An SHA-1 identifier of the node to return.
   * @return Persistent node with given id.
   */
  def node(id: Integer160): Option[KnownNodeInfo]

  /**
   * Returns traversable collection of all persisted nodes.
   *
   * @return Traversable collection of all persisted nodes.
   */
  def nodes(): Traversable[KnownNodeInfo]

  /**
   * Returns traversable collection of all nodes within given bucket.
   *
   * @param bucket A bucket to read nodes for.
   * @return Traversable collection of all nodes within given bucket.
   */
  def nodes(bucket: Bucket): Traversable[KnownNodeInfo]

  /**
   * Returns sequence of maximum length N of stored nodes which ids are
   * by means of XOR operation closest to the given target 160-bit integer.
   *
   * @param N       Maximum number of nodes to return.
   * @param target  Target number to get closest nodes to.
   * @return sequence of stored nodes.
   */
  def closest(N: Int, target: Integer160): Seq[KnownNodeInfo] = {
    this.nodes(this.bucket(target)) match {
      case nn: Traversable[KnownNodeInfo] if nn.size < N =>
        this.nodes()
          .map(node => node -> (node.id ^ target)).toSeq
          .sortWith(_._2 < _._2)
          .take(N)
          .map(_._1)
      case nn: Traversable[KnownNodeInfo] if nn.size >= N =>
        nn.toSeq
    }
  }

  /**
   * Returns bucket which contains given id.
   *
   * @param id An id, which must be contained by bucket.
   * @return bucket which contains given id.
   */
  def bucket(id: Integer160): Bucket = this.buckets().find(id @: _).get

  /**
   * Returns traversable collection of all buckets.
   *
   * @return Traversable collection of all buckets.
   */
  def buckets(): Traversable[Bucket]

  /**
   * Returns traversable collection of all stored peers.
   *
   * @return traversable collection of all stored peers.
   */
  def peers(): Traversable[(Integer160, Peer)]
  /**
   * Returns traversable collection of peers associated with given infohash.
   *
   * @param infohash An infohash to get associated peers with.
   * @return traversable collection of peers associated with given infohash.
   */
  def peers(infohash: Integer160): Traversable[Peer]


  //////////////////////////////////////////////////////////////
  // PROPOSED IMPLEMENTATION BASED ON java.sql.PreparedStatement
  //////////////////////////////////////////////////////////////


  /**
   * Executes given [[java.sql.PreparedStatement]] to retrieve DHT node identifier.
   *
   * @param statement A statement to execute.
   * @return Some if there is an id in the storage, None otherwise.
   */
  protected def id(statement: PreparedStatement): Option[Integer160] = {
    using(statement.executeQuery()) { rs =>
      if (rs.next()) Some(new Integer160(rs.getBytes("id"))) else None
    }
  }

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
  protected def node(statement: PreparedStatement, id: Integer160): Option[KnownNodeInfo] = {
    statement.setBytes(1, id.toArray)
    using(statement.executeQuery()) { rs =>
      if (rs.next()) Some(this.read(rs)) else None
    }
  }

  /**
   * Executes given query and reads all its rows converting them into instances of
   * [[org.abovobo.dht.KnownNodeInfo]].
   *
   * @param statement A statement to execute.
   * @return Traversable collection of all persisted nodes.
   */
  protected def nodes(statement: PreparedStatement): Traversable[KnownNodeInfo] = {
    var nodes = List.empty[KnownNodeInfo]
    using(statement.executeQuery()) { rs =>
      while (rs.next()) {
        nodes ::= this.read(rs)
      }
    }
    nodes.reverse
  }

  /**
   * Sets statement parameters and executes query.
   *
   * Expected parameter mapping:
   * 1 BINARY bucket.start
   * 2 BINARY bucket.end
   *
   * @param statement A statement to execute.
   * @param bucket    An instance of bucket to get nodes for.
   * @return Persistent node with given id.
   */
  protected def nodes(statement: PreparedStatement, bucket: Bucket): Traversable[KnownNodeInfo] = {
    var nodes = List.empty[KnownNodeInfo]
    statement.setBytes(1, bucket.start.toArray)
    statement.setBytes(2, bucket.end.toArray)
    using(statement.executeQuery()) { rs =>
      while (rs.next()) {
        nodes ::= this.read(rs)
      }
    }
    nodes.reverse
  }

  /**
   * Executes given statement reading all rows into collection.
   *
   * @param statement A statement to execute.
   * @return Traversable collection of all buckets.
   */
  protected def buckets(statement: PreparedStatement): Traversable[Bucket] = {
    var buckets = List.empty[Bucket]
    var prev: (Integer160, Date) = null
    using(statement.executeQuery()) { rs =>
      while (rs.next()) {
        val id = new Integer160(rs.getBytes("id"))
        val seen = new Date(rs.getTimestamp("seen").getTime)
        if (prev != null) buckets ::= new Bucket(prev._1, id - 1, prev._2)
        prev = id -> seen
      }
    }
    if (prev != null) buckets ::= new Bucket(prev._1, Integer160.maxval, prev._2)
    buckets.reverse
  }

  /**
   * Executes given statement reading all rows into a collection.
   *
   * @param statement A statement to execute.
   * @return          Collection of peers.
   */
  protected def peers(statement: PreparedStatement): Traversable[(Integer160, Peer)] = {
    var peers = List.empty[(Integer160, Peer)]
    using(statement.executeQuery()) { rs =>
      while (rs.next()) {
        peers ::= new Integer160(rs.getBytes("infohash")) -> rs.getBytes("address")
      }
    }
    peers.reverse
  }

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
    var peers = List.empty[Peer]
    statement.setBytes(1, infohash.toArray)
    using(statement.executeQuery()) { rs =>
      while (rs.next()) {
        peers ::= rs.getBytes("address")
      }
    }
    peers.reverse
  }

  /**
   * Reads instance of [[org.abovobo.dht.KnownNodeInfo]] from given [[java.sql.ResultSet]].
   *
   * @param rs Valid [[java.sql.ResultSet]] instance.
   * @return New instance of [[org.abovobo.dht.KnownNodeInfo]] built from read data.
   */
  private def read(rs: ResultSet): KnownNodeInfo = {
    import org.abovobo.jdbc.Optional._
    new KnownNodeInfo(
      new Integer160(rs.getBytes("id")),
      rs.getBytes("address"),
      rs.timestamp("replied"),
      rs.timestamp("queried"),
      rs.getInt("failcount"))
  }
}

/**
 * Accompanying object containing string keys for storage queries.
 */
object Reader {
  val Q_GET_SELF_ID_NAME = "GET_SELF_ID"
  val Q_NODE_BY_ID_NAME = "NODE_BY_ID"
  val Q_ALL_NODES_NAME = "ALL_NODES"
  val Q_NODES_BY_BUCKET_NAME = "NODES_BY_BUCKET"
  val Q_ALL_BUCKETS_NAME = "ALL_BUCKETS"
  val Q_PEERS_NAME ="PEERS_BY_INFOHASH"
  val Q_ALL_PEERS_NAME = "ALL_PEERS"
}

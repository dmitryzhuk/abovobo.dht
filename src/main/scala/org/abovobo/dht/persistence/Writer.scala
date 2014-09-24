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

import java.sql.{PreparedStatement, Timestamp}

import org.abovobo.dht.Endpoint._
import org.abovobo.dht.message.Message.Kind._
import org.abovobo.dht.{KnownNodeInfo, NodeInfo, Peer}
import org.abovobo.integer.Integer160

import scala.concurrent.duration.FiniteDuration

/**
 * This trait defines method which modifies the state of DHT persistent storage.
 *
 * Each method presented by pair of public and protected methods.
 * Protected method receives prepared statement as an argument and performs
 * all necessary manipulations with it assuming documented layout of that
 * statement. Implementors will override public methods by redirecting execution
 * to protected methods supplying pre-instantiated [[java.sql.PreparedStatement]].
 *
 * None of these methods commit transaction. This must be done outside of the
 * [[org.abovobo.dht.persistence.Writer]].
 *
 * @author Dmitry Zhuk
 */
trait Writer {

  /**
   * Sets given id as DHT node identifier.
   *
   * @param id An SHA-1 identifier of DHT node.
   */
  def id(id: Integer160): Unit

  /**
   * Inserts new node.
   *
   * @param node    A node to insert into a storage.
   * @param kind    A kind of network message.
   */
  def insert(node: NodeInfo, kind: Kind): Unit

  /**
   * Inserts new bucket using given id as a lower bound.
   *
   * @param id    A lower-bound id of the new bucket.
   */
  def insert(id: Integer160): Unit

  /**
   * Updates existing node.
   *
   * @param node    A node which sent network message.
   * @param pn      A corresponding persistent node.
   * @param kind    A kind of network message.
   */
  def update(node: NodeInfo, pn: KnownNodeInfo, kind: Kind): Unit

  /**
   * Deletes the node with given id.
   *
   * @param id    An id of node to delete.
   */
  def delete(id: Integer160): Unit

  /**
   * Touch given bucket setting its 'seen' property to 'now()'
   *
   * @param id    A lower-bound of a bucket to touch.
   */
  def touch(id: Integer160): Unit

  /**
   * Deletes all buckets. All nodes will be consequently removed by cascading.
   */
  def drop(): Unit

  /**
   * Stores association between particular infohash and peer info.
   *
   * @param infohash  An infohash to associate peer info with.
   * @param peer      A peer info to associated with given infohash.
   */
  def announce(infohash: Integer160, peer: Peer): Unit

  /**
   * Removes all infohash->peer associations older than provided lifetime duration.
   *
   * @param lifetime  A lifetime duration - all entries older than this will be removed.
   */
  def cleanup(lifetime: FiniteDuration): Unit


  //////////////////////////////////////////////////////////////
  // PROPOSED IMPLEMENTATION BASED ON java.sql.PreparedStatement
  //////////////////////////////////////////////////////////////

  /**
   * This method first executes drop statement to delete currently persisted DHT node identifier,
   * then sets parameter to save statement and executes it consequently.
   *
   * Expected parameter mapping:
   * 1 BINARY id
   *
   * @param drop A statement which drops existing id if any.
   * @param save A statement which inserts new id.
   * @param id   An identifier to set.
   */
  protected def id(drop: PreparedStatement, save: PreparedStatement, id: Integer160): Unit = {
    drop.executeUpdate()
    save.setBytes(1, id.toArray)
    save.executeUpdate()
  }


  /**
   * Sets statement parameters from given arguments and executes it in update mode.
   *
   * Expected parameter mapping:
   * 1 BINARY     node.id
   * 2 BINARY     node.address
   * 3 TIMESTAMP  System.currentTimeMillis (if kind == Reply)
   * 4 TIMESTAMP  System.currentTimeMillis (if kind == Query)
   *
   * @param statement A statement to execute.
   * @param node      A node instance to get properties from.
   * @param kind      A [[Kind]] represending
   *                  type of network communication occurred. Method will fail if
   *                  kind is Fail or Error
   */
  protected def insert(statement: PreparedStatement, node: NodeInfo, kind: Kind): Unit = {
    statement.setBytes(1, node.id.toArray)
    statement.setBytes(2, node.address)
    if (kind == Response) {
      statement.setTimestamp(3, new Timestamp(System.currentTimeMillis))
      statement.setNull(4, java.sql.Types.TIMESTAMP)
    } else if (kind == Query) {
      statement.setNull(3, java.sql.Types.TIMESTAMP)
      statement.setTimestamp(4, new Timestamp(System.currentTimeMillis))
    }
    statement.executeUpdate
  }

  /**
   * Sets statement parameters and executes it in update mode.
   *
   * Expected parameter mapping:
   * 1 BINARY     id
   *
   * @param statement A statement to execute.
   * @param id        Lower bound of new bucket to insert.
   */
  protected def insert(statement: PreparedStatement, id: Integer160): Unit = {
    statement.setBytes(1, id.toArray)
    statement.executeUpdate()
  }

  /**
   * Set statement parameters from given arguments and executes it in update mode.
   * This method WILL NOT change the bucket given node belongs to.
   * Note that if kind is Error this will update 'replied' timestamp leaving
   * failcount untouched.
   *
   * Expected parameter mapping:
   * 1 BINARY     node.address
   * 2 TIMESTAMP  System.currentTimeMillis (if kind == Reply or kind == Error)
   * 3 TIMESTAMP  System.currentTimeMillis (if kind == Query)
   * 4 INTEGER    node.failcount + 1 (if kind == Fail)
   * 5 BINARY     node.id
   *
   * @param statement A statement to execute.
   * @param node      A node instance to get properties from.
   * @param known     An instance of the same node with complete information before this update.
   * @param kind      A [[Kind]] represending
   *                  type of network communication occurred. Method will fail if
   *                  kind is Fail or Error
   */
  protected def update(statement: PreparedStatement, node: NodeInfo, known: KnownNodeInfo, kind: Kind): Unit = {
    statement.setBytes(1, node.address)
    kind match {
      case Response | Error =>
        statement.setTimestamp(2, new Timestamp(System.currentTimeMillis))
        statement.setTimestamp(3, known.queried.map(d => new Timestamp(d.getTime)).orNull)
        statement.setInt(4, 0)
      case Query =>
        statement.setTimestamp(2, known.replied.map(d => new Timestamp(d.getTime)).orNull)
        statement.setTimestamp(3, new Timestamp(System.currentTimeMillis))
        statement.setInt(4, known.failcount)
      case Fail =>
        statement.setTimestamp(2, known.replied.map(d => new Timestamp(d.getTime)).orNull)
        statement.setTimestamp(3, known.queried.map(d => new Timestamp(d.getTime)).orNull)
        statement.setInt(4, known.failcount + 1)
    }
    statement.setBytes(5, node.id.toArray)
    statement.executeUpdate()
  }

  /**
   * Sets statement parameters and executes it in update mode.
   *
   * Expected parameter mapping:
   * 1 BINARY     id
   *
   * @param statement A statement to execute.
   * @param id        An identifier of the node to delete.
   */
  protected def delete(statement: PreparedStatement, id: Integer160): Unit = {
    statement.setBytes(1, id.toArray)
    statement.executeUpdate()
  }

  /**
   * Sets statement parameters and executes it in update mode.
   *
   * Expected parameter mapping:
   * 1 BINARY     id
   *
   * @param statement A statement to execute.
   * @param id        Lower bound of new bucket to update.
   */
  protected def touch(statement: PreparedStatement, id: Integer160): Unit = {
    statement.setBytes(1, id.toArray)
    statement.executeUpdate()
  }

  /**
   * Simply executes given statement.
   *
   * @param statement A statement to execute.
   */
  protected def drop(statement: PreparedStatement): Unit = {
    statement.executeUpdate()
  }

  /**
   * Sets statement parameters and executes it in update mode.
   *
   * Expected parameter mapping:
   * 1 BINARY     infohash
   * 2 BINARY     peer
   *
   * @param statement A statement to execute.
   * @param infohash  An infohash to set as parameter 1
   * @param peer      A peer info to set as parameter 2
   */
  protected def announce(statement: PreparedStatement, infohash: Integer160, peer: Peer): Unit = {
    statement.setBytes(1, infohash.toArray)
    statement.setBytes(2, peer)
    statement.executeUpdate()
  }

  /**
   * Sets statement parameters and executes it in update mode.
   *
   * Expected parameter mapping:
   *
   * 1 LONG     lifetime (in seconds)
   *
   * @param statement A statement to execute.
   * @param lifetime  Lifetime duration.
   */
  protected def cleanup(statement: PreparedStatement, lifetime: FiniteDuration): Unit = {
    statement.setLong(1, lifetime.toSeconds)
    statement.executeUpdate()
  }
}

/**
 * Accompanying object containing string keys for storage queries.
 */
object Writer {
  val Q_SET_SELF_ID_NAME = "SET_SELF_ID"
  val Q_DROP_SELF_ID_NAME = "DROP_SELF_ID"
  val Q_INSERT_NODE_NAME = "INSERT_NODE"
  val Q_UPDATE_NODE_NAME = "UPDATE_NODE"
  val Q_DELETE_NODE_NAME = "DELETE_NODE"
  val Q_INSERT_BUCKET_NAME = "INSERT_BUCKET"
  val Q_TOUCH_BUCKET_NAME = "TOUCH_BUCKET"
  val Q_DROP_ALL_BUCKETS_NAME = "DROP_ALL_BUCKETS"
  val Q_ANNOUNCE_PEER_NAME = "ANNOUNCE_PEER"
  val Q_CLEANUP_PEERS_NAME = "CLEANUP_PEERS"
}

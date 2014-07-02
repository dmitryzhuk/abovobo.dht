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

import org.abovobo.dht.{Peer, Message, KnownNode, Node}
import org.abovobo.dht.Endpoint._
import org.abovobo.integer.Integer160
import Message.Kind._
import java.sql.{Timestamp, PreparedStatement}
import scala.concurrent.duration.FiniteDuration

/**
 * This trait defines method which modify the state of DHT persistent storage.
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
   * Inserts new node into the given bucket.
   *
   * @param node    A node to insert into a storage.
   * @param bucket  A bucket to add this node to.
   * @param kind    A kind of network message.
   */
  def insert(node: Node, bucket: Integer160, kind: Kind): Unit

  /**
   * Sets statement parameters from given arguments and executes it in update mode.
   *
   * Expected parameter mapping:
   * 1 BINARY     node.id
   * 2 BINARY     bucket
   * 3 BINARY     node.address
   * 4 TIMESTAMP  System.currentTimeMillis (if kind == Reply)
   * 5 TIMESTAMP  System.currentTimeMillis (if kind == Query)
   *
   * @param statement A statement to execute.
   * @param node      A node instance to get properties from.
   * @param bucket    A bucket to which this node will be inserted.
   * @param kind      A [[org.abovobo.dht.Message.Kind.Kind]] represending
   *                  type of network communication occurred. Method will fail if
   *                  kind is Fail or Error
   */
  protected def insert(statement: PreparedStatement, node: Node, bucket: Integer160, kind: Kind): Unit = {
    statement.setBytes(1, node.id.toArray)
    statement.setBytes(2, bucket.toArray)
    statement.setBytes(3, node.address)
    if (kind == Response) {
      statement.setTimestamp(4, new Timestamp(System.currentTimeMillis))
      statement.setNull(5, java.sql.Types.TIMESTAMP)
    } else if (kind == Query) {
      statement.setNull(4, java.sql.Types.TIMESTAMP)
      statement.setTimestamp(5, new Timestamp(System.currentTimeMillis))
    }
    statement.executeUpdate
  }

  /**
   * Updates existing node.
   *
   * @param node    A node which sent network message.
   * @param pn      A corresponding persistent node.
   * @param kind    A kind of network message.
   */
  def update(node: Node, pn: KnownNode, kind: Kind): Unit

  /**
   * Set statement parameters from given arguments and executes it in update mode.
   * This method WILL NOT change the bucket given node belongs to.
   * Note that if kind is Error this will update 'replied' timestamp leaving
   * failcount untouched.
   *
   * Expected parameter mapping:
   * 1 BINARY     node.address
   * 3 TIMESTAMP  System.currentTimeMillis (if kind == Reply or kind == Error)
   * 4 TIMESTAMP  System.currentTimeMillis (if kind == Query)
   * 5 INTEGER    node.failcount + 1 (if kind == Fail)
   * 6 BINARY     node.id
   *
   * @param statement A statement to execute.
   * @param node      A node instance to get properties from.
   * @param kind      A [[org.abovobo.dht.Message.Kind.Kind]] represending
   *                  type of network communication occurred. Method will fail if
   *                  kind is Fail or Error
   */
  protected def update(statement: PreparedStatement, node: Node, pn: KnownNode, kind: Kind): Unit = {
    statement.setBytes(1, node.address)
    kind match {
      case Response | Error =>
        statement.setTimestamp(2, new Timestamp(System.currentTimeMillis))
        statement.setTimestamp(3, pn.queried.map(d => new Timestamp(d.getTime)).orNull)
        statement.setInt(4, pn.failcount)
      case Query =>
        statement.setTimestamp(2, pn.replied.map(d => new Timestamp(d.getTime)).orNull)
        statement.setTimestamp(3, new Timestamp(System.currentTimeMillis))
        statement.setInt(4, pn.failcount)
      case Fail =>
        statement.setTimestamp(2, pn.replied.map(d => new Timestamp(d.getTime)).orNull)
        statement.setTimestamp(3, pn.queried.map(d => new Timestamp(d.getTime)).orNull)
        statement.setInt(4, pn.failcount + 1)
    }
    statement.setBytes(5, node.id.toArray)
    statement.executeUpdate()
  }

  /**
   * Moves existing node into a given bucket.
   *
   * @param node    A node to move.
   * @param bucket  A bucket to move node to.
   */
  def move(node: KnownNode, bucket: Integer160): Unit

  /**
   * Sets statement parameters and executes it in update mode. Note,
   * that this method WILL ONLY CHANGE bucket.
   *
   * Expected parameter mapping:
   * 1 BINARY     bucket
   * 2 BINARY     node.id
   *
   * @param statement A statement to execute.
   * @param node      A node instance to get properties from.
   * @param bucket    A bucket to which this node will be inserted.
   */
  protected def move(statement: PreparedStatement, node: KnownNode, bucket: Integer160): Unit = {
    statement.setBytes(1, bucket.toArray)
    statement.setBytes(2, node.id.toArray)
    statement.executeUpdate()
  }
  /**
   * Deletes the node with given id.
   *
   * @param id    An id of node to delete.
   */
  def delete(id: Integer160): Unit

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
   * Inserts new bucket using given id as a lower bound.
   *
   * @param id    A lower-bound id of the new bucket.
   */
  def insert(id: Integer160): Unit


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
   * Touch given bucket setting its 'seen' property to 'now()'
   *
   * @param id    A lower-bound of a bucket to touch.
   */
  def touch(id: Integer160): Unit

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
   * Deletes all buckets. All nodes will be consequently removed by cascading.
   */
  def drop(): Unit

  /**
   * Simply executes given statement.
   *
   * @param statement A statement to execute.
   */
  protected def drop(statement: PreparedStatement): Unit = {
    statement.executeUpdate()
  }

  /**
   * Stores association between particular infohash and peer info.
   *
   * @param infohash  An infohash to associate peer info with.
   * @param peer      A peer info to associated with given infohash.
   */
  def announce(infohash: Integer160, peer: Peer): Unit

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
   * Removes all infohash->peer associations older than provided lifetime duration.
   *
   * @param lifetime  A lifetime duration - all entries older than this will be removed.
   */
  def cleanup(lifetime: FiniteDuration): Unit

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

  /**
   * Allows to execute block of code within transaction.
   *
   * @param f   A code block to execute.
   * @tparam T  Return type of the code block.
   * @return    A value yielded by code block.
   */
  def transaction[T](f: => T): T
}

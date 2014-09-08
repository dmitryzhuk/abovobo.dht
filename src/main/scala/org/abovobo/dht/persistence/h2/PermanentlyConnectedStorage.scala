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

import java.sql.{PreparedStatement, Connection}

import org.abovobo.dht.message.Message.Kind._
import org.abovobo.dht._
import org.abovobo.integer.Integer160
import org.h2.tools.RunScript

import scala.concurrent.duration.FiniteDuration

/**
 * H2-based implementation of [[persistence.PermanentlyConnectedStorage]]
 *
 * @param connection A connection to be used by this storage.
 */
class PermanentlyConnectedStorage(connection: Connection) extends persistence.PermanentlyConnectedStorage(connection) {

  //////////////////
  /// READER METHODS
  //////////////////

  /** @inheritdoc */
  override def id() =
    this.id(this.statement(persistence.Reader.Q_GET_SELF_ID_NAME))

  /** @inheritdoc */
  override def node(id: Integer160) =
    this.node(this.statement(persistence.Reader.Q_NODE_BY_ID_NAME), id)

  /** @inheritdoc */
  override def nodes() =
    this.nodes(this.statement(persistence.Reader.Q_ALL_NODES_NAME))

  /** @inheritdoc */
  override def nodes(bucket: Bucket) =
    this.nodes(this.statement(persistence.Reader.Q_NODES_BY_BUCKET_NAME), bucket)

  /** @inheritdoc */
  override def buckets() =
    this.buckets(this.statement(persistence.Reader.Q_ALL_BUCKETS_NAME))

  /** @inheritdoc */
  override def peers(infohash: Integer160) =
    this.peers(this.statement(persistence.Reader.Q_PEERS_NAME), infohash)

  /** @inheritdoc */
  override def peers() =
    this.peers(this.statement(persistence.Reader.Q_ALL_PEERS_NAME))

  //////////////////
  /// WRITER METHODS
  //////////////////

  /** @inheritdoc */
  override def id(id: Integer160) = this.id(
    this.statement(persistence.Writer.Q_DROP_SELF_ID_NAME),
    this.statement(persistence.Writer.Q_SET_SELF_ID_NAME),
    id)

  /** @inheritdoc */
  override def insert(node: NodeInfo, kind: Kind) =
    this.insert(this.statement(persistence.Writer.Q_INSERT_NODE_NAME), node, kind)

  /** @inheritdoc */
  override def insert(id: Integer160) =
    this.insert(this.statement(persistence.Writer.Q_INSERT_BUCKET_NAME), id)

  /** @inheritdoc */
  override def update(node: NodeInfo, pn: KnownNodeInfo, kind: Kind) =
    this.update(this.statement(persistence.Writer.Q_UPDATE_NODE_NAME), node, pn, kind)

  /** @inheritdoc */
  override def delete(id: Integer160) =
    this.delete(this.statement(persistence.Writer.Q_DELETE_NODE_NAME), id)

  /** @inheritdoc */
  override def touch(id: Integer160) =
    this.touch(this.statement(persistence.Writer.Q_TOUCH_BUCKET_NAME), id)

  /** @inheritdoc */
  override def drop() =
    this.drop(this.statement(persistence.Writer.Q_DROP_ALL_BUCKETS_NAME))

  /** @inheritdoc */
  override def announce(infohash: Integer160, peer: Peer) =
    this.announce(this.statement(persistence.Writer.Q_ANNOUNCE_PEER_NAME), infohash, peer)

  /** @inheritdoc */
  override def cleanup(lifetime: FiniteDuration) =
    this.cleanup(this.statement(persistence.Writer.Q_CLEANUP_PEERS_NAME), lifetime)

  /** @inheritdoc */
  override def execute(script: java.io.Reader): Unit = RunScript.execute(this.connection, script)

  /** @inheritdoc */
  override protected def prepare(): Map[String, PreparedStatement] =
    Map(
      /// READER QUERIES
      persistence.Reader.Q_GET_SELF_ID_NAME -> this.connection.prepareStatement(Reader.Q_GET_SELF_ID_STMT),
      persistence.Reader.Q_NODE_BY_ID_NAME -> this.connection.prepareStatement(Reader.Q_NODE_BY_ID_STMT),
      persistence.Reader.Q_ALL_NODES_NAME -> this.connection.prepareStatement(Reader.Q_ALL_NODES_STMT),
      persistence.Reader.Q_NODES_BY_BUCKET_NAME -> this.connection.prepareStatement(Reader.Q_NODES_BY_BUCKET_STMT),
      persistence.Reader.Q_ALL_BUCKETS_NAME -> this.connection.prepareStatement(Reader.Q_ALL_BUCKETS_STMT),
      persistence.Reader.Q_PEERS_NAME -> this.connection.prepareStatement(Reader.Q_PEERS_STMT),
      persistence.Reader.Q_ALL_PEERS_NAME -> this.connection.prepareStatement(Reader.Q_ALL_PEERS_STMT),
      /// WRITER QUERIES
      persistence.Writer.Q_SET_SELF_ID_NAME -> this.connection.prepareStatement(Writer.Q_SET_SELF_ID_STMT),
      persistence.Writer.Q_DROP_SELF_ID_NAME -> this.connection.prepareStatement(Writer.Q_DROP_SELF_ID_STMT),
      persistence.Writer.Q_INSERT_NODE_NAME -> this.connection.prepareStatement(Writer.Q_INSERT_NODE_STMT),
      persistence.Writer.Q_UPDATE_NODE_NAME -> this.connection.prepareStatement(Writer.Q_UPDATE_NODE_STMT),
      persistence.Writer.Q_DELETE_NODE_NAME -> this.connection.prepareStatement(Writer.Q_DELETE_NODE_STMT),
      persistence.Writer.Q_INSERT_BUCKET_NAME -> this.connection.prepareStatement(Writer.Q_INSERT_BUCKET_STMT),
      persistence.Writer.Q_TOUCH_BUCKET_NAME -> this.connection.prepareStatement(Writer.Q_TOUCH_BUCKET_STMT),
      persistence.Writer.Q_DROP_ALL_BUCKETS_NAME -> this.connection.prepareStatement(Writer.Q_DROP_ALL_BUCKETS_STMT),
      persistence.Writer.Q_ANNOUNCE_PEER_NAME -> this.connection.prepareStatement(Writer.Q_ANNOUNCE_PEER_STMT),
      persistence.Writer.Q_CLEANUP_PEERS_NAME -> this.connection.prepareStatement(Writer.Q_CLEANUP_PEERS_STMT)
    )
}

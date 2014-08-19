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

import java.sql.PreparedStatement

import org.abovobo.dht.message.Message.Kind._
import org.abovobo.dht._
import org.abovobo.integer.Integer160
import org.abovobo.jdbc.Closer._
import org.h2.tools.RunScript

import scala.concurrent.duration.FiniteDuration

/**
 * H2-based implementation of [[persistence.DynamicallyConnectedStorage]]
 *
 * @param ds  DataSource instance used to retrieve connection.
 */
class DynamicallyConnectedStorage(ds: persistence.DataSource) extends persistence.DynamicallyConnectedStorage(ds) {

  /** Executes given script using H2 [[org.h2.tools.RunScript]] utility */
  override def execute(script: java.io.Reader): Unit = using(this.connection) { c =>
    RunScript.execute(c, script)
  }

  //////////////////
  /// READER METHODS
  //////////////////

  /** @inheritdoc */
  override def id() = this.invoke(Reader.Q_GET_SELF_ID_STMT, this.id)
  /** @inheritdoc */
  override def node(id: Integer160) = this.invoke(Reader.Q_NODE_BY_ID_STMT, this.node(_, id))
  /** @inheritdoc */
  override def nodes() = this.invoke(Reader.Q_ALL_NODES_STMT, this.nodes)
  /** @inheritdoc */
  override def nodes(bucket: Bucket) = this.invoke(Reader.Q_NODES_BY_BUCKET_STMT, this.nodes(_, bucket))
  /** @inheritdoc */
  override def buckets() = this.invoke(Reader.Q_ALL_BUCKETS_STMT, this.buckets)
  /** @inheritdoc */
  override def peers(infohash: Integer160) = this.invoke(Reader.Q_ALL_PEERS_STMT, this.peers(_, infohash))

  //////////////////
  /// WRITER METHODS
  //////////////////

  /** @inheritdoc */
  override def id(id: Integer160) = using(this.connection) { c =>
    using(c.prepareStatement(Writer.Q_DROP_SELF_ID_STMT)) { _.executeUpdate() }
    using(c.prepareStatement(Writer.Q_SET_SELF_ID_STMT)) { s =>
      s.setBytes(1, id.toArray)
      s.executeUpdate()
    }
  }
  /** @inheritdoc */
  override def insert(node: NodeInfo, kind: Kind) = this.invoke(Writer.Q_INSERT_NODE_STMT, this.insert(_, node, kind))
  /** @inheritdoc */
  override def insert(id: Integer160) = this.invoke(Writer.Q_INSERT_BUCKET_STMT, this.insert(_, id))
  /** @inheritdoc */
  override def update(node: NodeInfo, pn: KnownNodeInfo, kind: Kind) =
    this.invoke(Writer.Q_UPDATE_NODE_STMT, this.update(_, node, pn, kind))
  /** @inheritdoc */
  override def delete(id: Integer160) = this.invoke(Writer.Q_DELETE_NODE_STMT, this.delete(_, id))
  /** @inheritdoc */
  override def touch(id: Integer160) = this.invoke(Writer.Q_TOUCH_BUCKET_STMT, this.touch(_, id))
  /** @inheritdoc */
  override def drop() = this.invoke(Writer.Q_DROP_ALL_BUCKETS_STMT, this.drop)
  /** @inheritdoc */
  override def announce(infohash: Integer160, peer: Peer) =
    this.invoke(Writer.Q_ANNOUNCE_PEER_STMT, this.announce(_, infohash, peer))
  /** @inheritdoc */
  override def cleanup(lifetime: FiniteDuration) = this.invoke(Writer.Q_CLEANUP_PEERS_STMT, this.cleanup(_, lifetime))
}

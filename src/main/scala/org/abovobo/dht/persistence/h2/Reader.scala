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

import org.abovobo.dht.persistence
import org.abovobo.integer.Integer160

/**
 * Actual implementation of [[persistence.Reader]] trait.
 */
class Reader(connection: Connection) extends persistence.Storage(connection) with persistence.Reader {

  /** @inheritdoc */
  override def id() = this.id(this.statements("getId"))

  /** @inheritdoc */
  override def nodes() = this.nodes(this.statements("allNodes"))

  /** @inheritdoc */
  override def node(id: Integer160) = this.node(this.statements("nodeById"), id)

  /** @inheritdoc */
  override def bucket(id: Integer160) = this.bucket(this.statements("nodesByBucket"), id)

  /** @inheritdoc */
  override def buckets() = this.buckets(this.statements("allBuckets"))

  /** @inheritdoc */
  override def peers(infohash: Integer160) = this.peers(this.statements("peers"), infohash)

  /** @inheritdoc */
  override protected def prepare() = {
    val c = this.connection
    Map(
      "getId" -> c.prepareStatement("select * from self"),

      "allNodes" -> c.prepareStatement("select * from node"),
      "nodeById" -> c.prepareStatement("select * from node where id=?"),
      "nodesByBucket" -> c.prepareStatement("select * from node where bucket=?"),

      "allBuckets" -> c.prepareStatement("select * from bucket"),

      "peers" -> c.prepareStatement("select * from peer where infohash=?"))
  }

}

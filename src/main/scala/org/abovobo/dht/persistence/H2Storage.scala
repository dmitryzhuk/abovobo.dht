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
import org.abovobo.dht.Node
import org.abovobo.dht.network.Message.Kind._

/**
 * Actual implementation of [[org.abovobo.dht.persistence.Storage]] which uses H2.
 *
 * @param uri     JDBC connection string.
 *
 * @author Dmitry Zhuk
 */
class H2Storage(uri: String) extends Storage("org.h2.Driver", uri) {

  /////////////////
  // READER METHODS
  /////////////////

  /** @inheritdoc */
  override protected def id() = this.id(this.statements("getId"))

  /** @inheritdoc */
  override protected def nodes() = this.nodes(this.statements("allNodes"))

  /** @inheritdoc */
  override protected def node(id: Integer160) = this.node(this.statements("nodeById"), id)

  /** @inheritdoc */
  override protected def bucket(id: Integer160) = this.bucket(this.statements("nodesByBucket"), id)

  /** @inheritdoc */
  override protected def buckets() = this.buckets(this.statements("allBuckets"))

  /////////////////
  // WRITER METHODS
  /////////////////

  /** @inheritdoc */
  override protected def id(id: Integer160) =
    this.id(this.statements("dropId"), this.statements("setId"), id)

  /** @inheritdoc */
  override protected def insert(node: Node, bucket: Integer160, kind: Kind) =
    this.insert(this.statements("insertNode"), node, bucket, kind)

  /** @inheritdoc */
  override protected def update(node: PersistentNode, kind: Kind) =
    this.update(this.statements("updateNode"), node, kind)

  /** @inheritdoc */
  override protected def move(node: PersistentNode, bucket: Integer160) =
    this.move(this.statements("moveNode"), node, bucket)

  /** @inheritdoc */
  override protected def delete(id: Integer160) =
    this.delete(this.statements("deleteNode"), id)

  /** @inheritdoc */
  override protected def insert(id: Integer160) =
    this.insert(this.statements("insertBucket"), id)

  /** @inheritdoc */
  override protected def touch(id: Integer160) =
    this.touch(this.statements("touchNode"), id)

  /** @inheritdoc */
  override protected def drop() =
    this.drop(this.statements("deleteAllBuckets"))

  ///////////////////////////
  // ABSTRACT STORAGE METHODS
  ///////////////////////////

  /** @inheritdoc */
  override protected def prepare() = {
    val c = this.connection
    Map(
      "setId" -> c.prepareStatement("insert into self(id) values(?)"),
      "dropId" -> c.prepareStatement("delete from self"),
      "getId" -> c.prepareStatement("select * from self"),

      "allNodes" -> c.prepareStatement("select * from node"),
      "nodeById" -> c.prepareStatement("select * from node where id=?"),
      "nodesByBucket" -> c.prepareStatement("select * from node where bucket=?"),
      "insertNode" -> c.prepareStatement(
        "insert into node(id, bucket, ipv4u, ipv4t, ipv6u, ipv6t, replied, queried) values(?, ?, ?, ?, ?, ?, ?, ?)"),
      "updateNode" -> c.prepareStatement(
        "update node set ipv4u=?, ipv4t=?, ipv6u=?, ipv6t=?, replied=?, queried=?, failcount=? where id=?"),
      "moveNode" -> c.prepareStatement("update node set bucket=? where id=?"),
      "deleteNode" -> c.prepareStatement("delete from node where id=?"),

      "allBuckets" -> c.prepareStatement("select * from bucket order by id"),
      "insertBucket" -> c.prepareStatement("insert into bucket(id, seen) values(?, now())"),
      "touchBucket" -> c.prepareStatement("update bucket set seen=now() where id=?"),
      "deleteAllBuckets" -> c.prepareStatement("delete from bucket"))
  }
}

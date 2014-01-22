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
import org.abovobo.dht.{Message, PersistentNode, Node}
import Message.Kind._
import org.abovobo.jdbc.Transaction

/**
 * Actual implementation of [[org.abovobo.dht.persistence.Storage]] which uses H2.
 *
 * @param uri     JDBC connection string.
 *
 * @author Dmitry Zhuk
 */
class H2Storage(uri: String) extends Storage("org.h2.Driver", uri) with Reader with Writer {

  /////////////////
  // READER METHODS
  /////////////////

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

  /////////////////
  // WRITER METHODS
  /////////////////

  /** @inheritdoc */
  override def id(id: Integer160) =
    this.id(this.statements("dropId"), this.statements("setId"), id)

  /** @inheritdoc */
  override def insert(node: Node, bucket: Integer160, kind: Kind) =
    this.insert(this.statements("insertNode"), node, bucket, kind)

  /** @inheritdoc */
  override def update(node: Node, pn: PersistentNode, kind: Kind) =
    this.update(this.statements("updateNode"), node, pn, kind)

  /** @inheritdoc */
  override def move(node: PersistentNode, bucket: Integer160) =
    this.move(this.statements("moveNode"), node, bucket)

  /** @inheritdoc */
  override def delete(id: Integer160) =
    this.delete(this.statements("deleteNode"), id)

  /** @inheritdoc */
  override def insert(id: Integer160) =
    this.insert(this.statements("insertBucket"), id)

  /** @inheritdoc */
  override def touch(id: Integer160) =
    this.touch(this.statements("touchBucket"), id)

  /** @inheritdoc */
  override def drop() =
    this.drop(this.statements("deleteAllBuckets"))

  ///////////////////////////
  // ABSTRACT STORAGE METHODS
  ///////////////////////////

  /**
   * @inheritdoc
   *
   * Delegates execution to [[org.abovobo.jdbc.Transaction.transaction()]]
   */
  override def transaction[T](f: => T): T = Transaction.transaction(this.connection)(f)

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
        "insert into node(id, bucket, address, replied, queried) values(?, ?, ?, ?, ?)"),
      "updateNode" -> c.prepareStatement(
        "update node set address=?, replied=?, queried=?, failcount=? where id=?"),
      "moveNode" -> c.prepareStatement("update node set bucket=? where id=?"),
      "deleteNode" -> c.prepareStatement("delete from node where id=?"),

      "allBuckets" -> c.prepareStatement("select * from bucket order by id"),
      "insertBucket" -> c.prepareStatement("insert into bucket(id, seen) values(?, now())"),
      "touchBucket" -> c.prepareStatement("update bucket set seen=now() where id=?"),
      "deleteAllBuckets" -> c.prepareStatement("delete from bucket"))
  }
}
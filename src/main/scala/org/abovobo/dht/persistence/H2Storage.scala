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
import org.abovobo.dht.{Peer, Message, PersistentNode, Node}
import Message.Kind._
import org.abovobo.jdbc.Transaction
import scala.concurrent.duration.FiniteDuration
import java.io.File
import java.sql.DriverManager
import org.h2.tools.RunScript
import java.io.InputStreamReader
import java.sql.Connection
import javax.sql.DataSource
import org.h2.jdbcx.JdbcConnectionPool

object H2DataSource {
  def createDatabase(fsLocation: String): DataSource = {
    import org.abovobo.jdbc.Closer._
    import org.abovobo.arm._

    Class.forName("org.h2.Driver")
  
    val dbFile = location2file(fsLocation)
      
    if (dbFile.exists) throw new IllegalArgumentException("File arlready exists")
    
    using(DriverManager.getConnection("jdbc:h2:" + fsLocation)) { connection =>
      arm.using(this.getClass.getResourceAsStream("tables.sql")) { tablesDef => 
        RunScript.execute(connection, new InputStreamReader(tablesDef))     
      }  
    }
    
    H2DataSource(fsLocation)
  }
    
  def apply(fsLocation: String): DataSource = {
    JdbcConnectionPool.create(createUrl(fsLocation), "", "")
  }
  
  def open(fsLocation: String, createIfNotExists: Boolean = false): DataSource = {    
    if (!location2file(fsLocation).exists && createIfNotExists) createDatabase(fsLocation) else H2DataSource(fsLocation) 
  }

  private def createUrl(fsLocation: String) = "jdbc:h2:" + fsLocation + ";SCHEMA=ipv4"

  private def location2file(location: String) = new File((if (location.startsWith("~")) System.getProperty("user.home") + location.substring(1) else location) + ".h2.db")
}

object H2Storage {
  def apply(connection: Connection): H2Storage = new H2Storage(connection)  
}

/**
 * Actual implementation of [[org.abovobo.dht.persistence.Storage]] which uses H2.
 *
 * @author Dmitry Zhuk
 */
class H2Storage(connection: Connection) extends Storage(connection) with Reader with Writer {

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

  /** @inheritdoc */
  override def peers(infohash: Integer160) = this.peers(this.statements("peers"), infohash)

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

  /** @inheritdoc */
  override def announce(infohash: Integer160, peer: Peer) =
    this.announce(this.statements("announcePeer"), infohash, peer)

  /** @inheritdoc */
  override def cleanup(lifetime: FiniteDuration) =
    this.cleanup(this.statements("cleanupPeers"), lifetime)

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
      "deleteAllBuckets" -> c.prepareStatement("delete from bucket"),

      "peers" -> c.prepareStatement("select * from peer where infohash=?"),
      "announcePeer" -> c.prepareStatement("merge into peer(infohash, address, announced) values(?, ?, now())"),
      "deletePeer" -> c.prepareStatement("delete from peer where infohash=? and address=?"),
      "cleanupPeers" -> c.prepareStatement("delete from peer where dateadd('SECOND', ?, announced) < now()"))
  }
}

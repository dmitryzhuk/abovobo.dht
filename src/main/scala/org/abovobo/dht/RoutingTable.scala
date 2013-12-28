/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht

import org.abovobo.integer.Integer160
import akka.actor.{Props, Actor}
import java.sql.{ResultSet, Timestamp, DriverManager}
import scala.concurrent.duration._
import org.abovobo.jdbc.Closer._
import org.abovobo.jdbc.Optional._
import org.abovobo.jdbc.Transaction._
import scala.collection.mutable.ArrayBuffer

/**
 * <p>This class represents routing table which is maintained by DHT node.</p>
 *
 * <p>
 * Here is the snippet from BEP005
 * (<a href="http://www.bittorrent.org/beps/bep_0005.html#id2">Routing Table</a>):
 * </p>
 *
 * <blockquote>
 * <p>
 * Every node maintains a routing table of known good nodes. The nodes in the routing table are used
 * as starting points for queries in the DHT. Nodes from the routing table are returned in response
 * to queries from other nodes.
 * </p>
 *
 * <p>
 * Not all nodes that we learn about are equal. Some are "good" and some are not. Many nodes using the DHT
 * are able to send queries and receive responses, but are not able to respond to queries from other nodes.
 * It is important that each node's routing table must contain only known good nodes. A good node is
 * a node has responded to one of our queries within the last 15 minutes. A node is also good if it has
 * ever responded to one of our queries and has sent us a query within the last 15 minutes.
 * After 15 minutes of inactivity, a node becomes questionable. Nodes become bad when they fail to respond to
 * multiple queries in a row. Nodes that we know are good are given priority over nodes with unknown status.
 * </p>
 *
 * <p>
 * The routing table covers the entire node ID space from 0 to 2<sup>160</sup>. The routing table is subdivided
 * into "buckets" that each cover a portion of the space. An empty table has one bucket with an ID space
 * range of min=0, max=2<sup>160</sup>. When a node with ID "N" is inserted into the table, it is placed within
 * the bucket that has min <= N < max. An empty table has only one bucket so any node must fit within it. Each
 * bucket can only hold K nodes, currently eight, before becoming "full." When a bucket is full of known good
 * nodes, no more nodes may be added unless our own node ID falls within the range of the bucket. In that case,
 * the bucket is replaced by two new buckets each with half the range of the old bucket and the nodes from the
 * old bucket are distributed among the two new ones. For a new table with only one bucket, the full bucket
 * is always split into two new buckets covering the ranges 0..2<sup>159</sup> and 2<sup>159</sup>..2<sup>160</sup>.
 * </p>
 *
 * <p>
 * When the bucket is full of good nodes, the new node is simply discarded. If any nodes in the bucket are
 * known to have become bad, then one is replaced by the new node. If there are any questionable nodes in
 * the bucket have not been seen in the last 15 minutes, the least recently seen node is pinged. If the pinged
 * node responds then the next least recently seen questionable node is pinged until one fails to respond
 * or all of the nodes in the bucket are known to be good. If a node in the bucket fails to respond to a ping,
 * it is suggested to try once more before discarding the node and replacing it with a new good node.
 * In this way, the table fills with stable long running nodes.
 * </p>
 *
 * <p>
 * Each bucket should maintain a "last changed" property to indicate how "fresh" the contents are.
 * When a node in a bucket is pinged and it responds, or a node is added to a bucket, or a node in a bucket
 * is replaced with another node, the bucket's last changed property should be updated. Buckets that have
 * not been changed in 15 minutes should be "refreshed." This is done by picking a random ID in the range
 * of the bucket and performing a find_nodes search on it. Nodes that are able to receive queries from other
 * nodes usually do not need to refresh buckets often. Nodes that are not able to receive queries from other
 * nodes usually will need to refresh all buckets periodically to ensure there are good nodes in their table
 * when the DHT is needed.
 * </p>
 *
 * <p>
 * Upon inserting the first node into its routing table and when starting up thereafter, the node should attempt
 * to find the closest nodes in the DHT to itself. It does this by issuing find_node messages to closer
 * and closer nodes until it cannot find any closer. The routing table should be saved between invocations
 * of the client software.
 * </p>
 * </blockquote>
 *
 * // --
 *
 * @constructor     Creates new instance of routing table with provided parameters.
 *
 * @param id        Own identifier associated with DHT node maintaining this table.
 * @param K         Max number of entries per bucket.
 * @param timeout   Time interval before node or bucket becomes questionable.
 *                  In documentation above is 15 minutes.
 * @param threshold Number of times a node must fail to respond before being marked as 'bad'.
 * @param path      File system path to database where routing table state is persisted.
 *
 * @author Dmitry Zhuk
 */
class RoutingTable(val id: Integer160,
                   val K: Int,
                   val timeout: Duration,
                   val threshold: Int,
                   val path: String) extends Actor {

  import RoutingTable._
  import RoutingTable.Method._
  import RoutingTable.Result._

  /**
   * @inheritdoc
   *
   * Handles RoutingTable Actor specific messages.
   */
  override def receive = {
    case GotQuery(node) => this.touch(node, Query)
    case GotReply(node) => this.touch(node, Reply)
    case GotFail(node)  => this.touch(node, Fail)
  }

  /**
   * @inheritdoc
   *
   * This override actually closes all prepared statements and database connection.
   * The potential [[java.sql.SQLException]] is swallowed by means of
   * org.abovobo.jdbc.Closer conversions, as it is totally unimportant at this point.
   */
  override def postStop() {
    this.statements.all foreach { _.dispose() }
    this.connection.dispose()
  }

  /**
   * This method "touches" given node considering given method of
   * receiving network message from that node. If given node does not
   * exist in this routing table it will be inserted, otherwise, node data
   * will be updated. If node is being inserted the buckets may be split
   * and node insertion still can be rejected if there was no room for
   * new node in the table.
   *
   * @param node    A node to "touch".
   * @param method  A method of receiving network message from the node.
   */
  private def touch(node: Node, method: Method): Result = {
    this.statements.nodeById.setBytes(1, node.id.toArray)
    using(this.statements.nodeById.executeQuery()) { rs =>
      transaction(this.connection) {
        if (rs.next()) {
          val pn = this.read(rs)
          this.update(node, pn, method)
        } else {
          // insert new node
          this.insert(node, method)
        }
      }
    }
  }

  private def update(node: Node, pn: PersistentNode, method: Method): Result = {
    // update node set ipv4u=?, ipv4t=?, ipv6u=?, ipv6t=?, replied=?, queried=?, failcount=? where id=?
    val s = this.statements.updateNode
    s.setBytes(1, node.ipv4u.map(_.data).getOrElse(pn.ipv4u.map(_.data).orNull))
    s.setBytes(2, node.ipv4t.map(_.data).getOrElse(pn.ipv4t.map(_.data).orNull))
    s.setBytes(3, node.ipv6u.map(_.data).getOrElse(pn.ipv6u.map(_.data).orNull))
    s.setBytes(4, node.ipv6t.map(_.data).getOrElse(pn.ipv6t.map(_.data).orNull))
    method match {
      case Reply =>
        s.setTimestamp(5, new Timestamp(System.currentTimeMillis))
        s.setTimestamp(6, pn.queried.map(d => new Timestamp(d.getTime)).orNull)
        s.setInt(7, pn.failcount)
      case Query =>
        s.setTimestamp(5, pn.replied.map(d => new Timestamp(d.getTime)).orNull)
        s.setTimestamp(6, new Timestamp(System.currentTimeMillis))
        s.setInt(7, pn.failcount)
      case Fail =>
        s.setTimestamp(5, pn.replied.map(d => new Timestamp(d.getTime)).orNull)
        s.setTimestamp(6, pn.queried.map(d => new Timestamp(d.getTime)).orNull)
        s.setInt(7, pn.failcount + 1)
    }
    s.setBytes(8, pn.id.toArray)
    Updated
  }

  private def insert(node: Node, method: Method): Result = {
    val buckets = new ArrayBuffer[Integer160]()
    using(this.statements.allBuckets.executeQuery()) { rs =>
      while (rs.next()) {
        buckets += new Integer160(rs.getBytes(1))
      }
    }
    val bucket = if (buckets.isEmpty) {
      // if there are no buckets exist we must insert zeroth bucket
      val zero = Integer160.zero
      this.statements.insertBucket.setBytes(1, zero.toArray)
      zero
    } else {
      // since zeroth element of this collection must be Integer160.zero
      // this always leads to valid index: we never can get -1 here.
      val index = buckets.lastIndexWhere(_ <= node.id)
      buckets(index)
    }
    val nodes = new ArrayBuffer[PersistentNode]()
    this.statements.nodesByBucket.setBytes(1, bucket.toArray)
    using(this.statements.nodesByBucket.executeQuery()) { rs =>
      while (rs.next()) {
        nodes += this.read(rs)
      }
    }
    if (nodes.size == this.K) {
      val good = nodes.filter(_.good)
      if (good.size == this.K) {
        // TODO Split if id of this node is within the bucket range and range is still splittable or reject
        Rejected
      } else {
        val bad = nodes.filter(_.bad)
        if (bad.size > 0) {
          this.statements.deleteNode.setBytes(1, bad.head.id.toArray)
          this.statements.deleteNode.executeUpdate
          this.insert(node, method, bucket)
          Replaced
        } else {
          val questionnable = nodes.filter(_.questionnable)
          // TODO Query questionnable nodes and properly defer new node insertion
          Deferred
        }
      }
    } else {
      this.insert(node, method, bucket)
      Inserted
    }
  }

  private def read(rs: ResultSet): PersistentNode =
    new PersistentNode(
      new Integer160(rs.getBytes("id")),
      rs.getBytesOrNone("ipv4u") map { new Endpoint(_) },
      rs.getBytesOrNone("ipv4t") map { new Endpoint(_) },
      rs.getBytesOrNone("ipv6u") map { new Endpoint(_) },
      rs.getBytesOrNone("ipv4t") map { new Endpoint(_) },
      new Integer160(rs.getBytes("bucket")),
      rs.getDateOrNone("replied"),
      rs.getDateOrNone("queried"),
      rs.getInt("failcount"),
      this.timeout,
      this.threshold)

  private def insert(node: Node, method: Method, bucket: Integer160): Unit = {
    val s = this.statements.insertNode
    s.setBytes(1, node.id.toArray)
    s.setBytes(2, bucket.toArray)
    s.setBytes(3, node.ipv4u.map(_.data).orNull)
    s.setBytes(4, node.ipv4t.map(_.data).orNull)
    s.setBytes(5, node.ipv6u.map(_.data).orNull)
    s.setBytes(6, node.ipv6t.map(_.data).orNull)
    if (method == Reply) {
      s.setTimestamp(7, new Timestamp(System.currentTimeMillis))
      s.setNull(8, java.sql.Types.TIMESTAMP)
    } else if (method == Query) {
      s.setNull(7, java.sql.Types.TIMESTAMP)
      s.setTimestamp(8, new Timestamp(System.currentTimeMillis))
    }
    s.executeUpdate
  }

  /// Initializes actual connection to H2 database in a lazy way
  private lazy val connection = {
    Class.forName("org.h2.Driver")
    DriverManager.getConnection("jdbc:h2:" + path + ";AUTOCOMMIT=OFF")
  }

  /// Collection of all statements used throughout RoutingTable functionality
  private object statements {
    private lazy val c = RoutingTable.this.connection

    // Node related queries
    lazy val nodeById = c.prepareStatement("select * from node where id=?")
    lazy val nodesByBucket = c.prepareStatement("select * from node where bucket=?")
    lazy val insertNode = c.prepareStatement(
      "insert into node(id, bucket, ipv4u, ipv4t, ipv6u, ipv6t, replied, queried) " +
        "values(?, ?, ?, ?, ?, ?, ?, ?)")
    lazy val deleteNode = c.prepareStatement("delete from node where id=?")
    lazy val updateNode = c.prepareStatement(
      "update node set ipv4u=?, ipv4t=?, ipv6u=?, ipv6t=?, replied=?, queried=?, failcount=? where id=?")
    lazy val moveNode = c.prepareStatement("update node set bucket=? where id=?")

    // Bucket related queries
    lazy val allBuckets = c.prepareStatement("select * from bucket order by id")
    lazy val insertBucket = c.prepareStatement("insert into bucket(id, seen) values(?, now())")
    lazy val touchBucket = c.prepareStatement("update bucket set seen=now() where id=?")

    lazy val all = Array(nodeById, allBuckets, insertBucket, touchBucket)
  }

}

/** Accompanying object */
object RoutingTable {

  /**
   * Factory which creates RoutingTable Actor Props instance.
   *
   * @param id        Own identifier associated with DHT node maintaining this table.
   * @param K         Max number of entries per bucket.
   * @param timeout   Time interval before node or bucket becomes questionable.
   *                  In documentation above is 15 minutes.
   * @param path      File system path to database where routing table state is persisted.
   *
   * @return          Properly configured Actor Props instance.
   */
  def props(id: Integer160, K: Int, timeout: Duration, path: String) =
    Props(classOf[RoutingTable], id, K, timeout, path)

  /**
   * Factory which creates RoutingTable Actor Props instance with default values:
   * K = 8, timeout = 15 minutes.
   *
   * @param id        Own identifier associated with DHT node maintaining this table.
   * @param path      File system path to database where routing table state is persisted.
   *
   * @return          Properly configured Actor Props instance.
   */
  def props(id: Integer160, path: String) = this.props(id, 8, 15 minutes, path)

  /**
   * This enumeration defines two possible methods of seeing the node:
   * Query means that method sent us query message, Reply means that node
   * just replied to our message. We must maintain this difference because
   * of algorithm of marking node as good or questionable as described
   * in class documentation.
   */
  object Method extends Enumeration {
    type Method = Value
    val Query, Reply, Fail = Value
  }

  /**
   * This enumeration defines three possible outcomes of touching the node:
   * Inserted for new node, Updated if node already existed in the table.
   * Replaced for the case when new node replaced existing bad node.
   * Rejected for the new node when there was no room in the table for it.
   * Deferred for the case when node processing has been deffered until some
   * checks with existing questionable nodes are done.
   */
  object Result extends Enumeration {
    type Result = Value
    val Inserted, Replaced, Updated, Rejected, Deferred = Value
  }

  /**
   * Basic trait for all messages supported by Table Actor.
   */
  sealed trait Message

  /**
   * This class represents a case when given node has replied to our query.
   *
   * @param node A Node instance of the subject.
   */
  case class GotReply(node: Node) extends Message

  /**
   * This class represents a case when a query has been received from given node.
   *
   * @param node A Node instance of the subject.
   */
  case class GotQuery(node: Node) extends Message

  /**
   * This class represents a case when node failed to respond to our query.
   *
   * @param node A Node instance in subject.
   */
  case class GotFail(node: Node) extends Message
}

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
import akka.actor.{Cancellable, Props, Actor}
import java.sql.{ResultSet, Timestamp, DriverManager}
import scala.concurrent.duration._
import scala.collection.mutable
import org.abovobo.dht.persistence.PersistentNode

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
 * @param K         Max number of entries per bucket.
 * @param timeout   Time interval before bucket becomes inactive or node becomes questionable.
 *                  In documentation above it is 15 minutes.
 * @param delay     A delay before deferred message must be redelivered to self.
 *                  This duration must normally be a bit longer when waiting node
 *                  reply timeout.
 * @param threshold Number of times a node must fail to respond before being marked as 'bad'.
 * @param db        File system path to database where routing table state is persisted.
 *
 * @author Dmitry Zhuk
 */
class RoutingTable(val K: Int,
                   val timeout: FiniteDuration,
                   val delay: FiniteDuration,
                   val threshold: Int,
                   val db: A) extends Actor {

  import RoutingTable._
  import RoutingTable.Result._

  /**
   * @inheritdoc
   *
   * Handles RoutingTable Actor specific messages.
   */
  override def receive = {
    case GotMessage(node, kind) => sender ! Report(node, this.touch(node, kind))
    case Refresh(_id)   => this.refresh(_id)
    case Reset()      => this.reset(); sender ! Done(this.id)
    case Set(_id)     => this.set(_id); sender ! Done(this.id)
    case Purge()        => this.purge()
  }

  /**
   * @inheritdoc
   *
   * This override checks if there is a stored node id and generates new one if not.
   */
  override def preStart() {
    import org.abovobo.jdbc.Closer._

    // upon start attempt to retrieve own id from the storage
    using(this.statements.getId.executeQuery()) { rs =>
      if (rs.next()) {
        // identifier found in the storage
        this.id = new Integer160(rs.getBytes(1))
      } else {
        // identifier is missing in the storage, make random one
        // and call `set` method to save it and purge all the data
        this.id = Integer160.random
        this.set(this.id)
      }
    }

    // upon start also perform refresh procedure for every existing bucket
    // and schedule the next refresh after configured idle timeout
    implicit val ec = this.context.system.dispatcher
    using(this.statements.allBuckets.executeQuery()) { rs =>
      while (rs.next()) {
        val id = new Integer160(rs.getBytes(1))
        self ! Refresh(id)
        this.cancellables.put(id, this.context.system.scheduler.scheduleOnce(this.timeout) {
          self ! Refresh(id)
        })
      }
    }
  }

  /**
   * @inheritdoc
   *
   * This override actually closes all prepared statements and database connection.
   * The potential [[java.sql.SQLException]] is swallowed by means of
   * org.abovobo.jdbc.Closer conversions, as it is totally unimportant at this point.
   */
  override def postStop() {
    import org.abovobo.jdbc.Closer._
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
   * @param kind    A kind of network message received from the node.
   */
  def touch(node: Node, kind: network.Message.Kind.Kind): Result = {

    import org.abovobo.jdbc.Transaction._
    import org.abovobo.jdbc.Closer._
    import network.Message.Kind._

    // try to read node with id of given network node
    this.statements.nodeById.setBytes(1, node.id.toArray)
    using(this.statements.nodeById.executeQuery()) { rs =>
      transaction(this.connection) {
        if (rs.next()) {
          // update existing node
          // note that in case of `Error` reply the node `replied` field
          // still will be updated in the same way as if it would properly replied.
          this.update(node, this.read(rs), kind)
          Updated
        } else if (kind != Error && kind != Fail) {
          // insert new node if method does not indicate failure or error
          // note that we won't insert a node if it has replied with error message
          this.insert(node, kind)
        } else {
          // reject operation if message indicates
          // failure or error reply of non-existent node
          Rejected
        }
      }
    }
  }

  /**
   * This method initiates bucket refresh sequence by choosing
   * random number from within the bucket range and sending `find_node`
   * message to network agent actor.
   *
   *@param id An identifier of the bucket to refresh.
   */
  def refresh(id: Integer160): Unit {
    // TODO FindNode must be sent to Newtorking Actor
  }

  /**
   * Generates new SHA-1 node id, drops all data and saves new id.
   */
  def reset(): Unit = this.set(Integer160.random)

  /**
   * Drops all data and saves new id in the storage.
   *
   * @param id New SHA-1 node identifier.
   */
  private def set(id: Integer160): Unit = {
    import org.abovobo.jdbc.Transaction._
    transaction(this.connection) {
      this.dropData()
      this.saveId(id)
      // actually change own ID only if there was no exceptions
      // in above manipulations with storage
      this.id = id
    }
  }

  /**
   * Deletes all data from database.
   */
  def purge(): Unit = {
    import org.abovobo.jdbc.Transaction._
    transaction(this.connection) {
      this.dropData()
    }
  }

  /**
   * Deletes all data from database out of the transaction.
   */
  private def dropData(): Unit = {
    this.statements.deleteAllBuckets.executeUpdate()
  }

  /**
   * Deletes existing self id from DB and saves given one out of the transaction.
   *
   * @param id New SHA-1 identifier to save.
   */
  private def saveId(id: Integer160): Unit = {
    this.statements.dropId.executeUpdate()
    this.statements.setId.setBytes(1, id.toArray)
    this.statements.setId.executeUpdate()
  }

  /**
   * Touches a bucket with given id resetting its last seen time stamp and scheduling Refresh event.
   *
   * @param bucket A bucket to touch.
   */
  private def touch(bucket: Integer160) = {
    // update bucket set seen=now() where id=?
    this.statements.touchBucket.setBytes(1, bucket.toArray)
    this.statements.touchBucket.executeUpdate()
    this.cancellables(bucket).cancel()
    implicit val ec = this.context.system.dispatcher
    this.cancellables(bucket) = this.context.system.scheduler.scheduleOnce(this.timeout) {
      self ! Refresh(id)
    }
  }

  /**
   * Reads instance of [[PersistentNode]] from given [[java.sql.ResultSet]].
   *
   * @param rs Valid [[java.sql.ResultSet]] instance.
   * @return new instance of [[PersistentNode]] built from read data.
   */
  private def read(rs: ResultSet): PersistentNode = {
    import org.abovobo.jdbc.Optional._
    new PersistentNode(
      new Integer160(rs.getBytes("id")),
      rs.bytes("ipv4u") map { new Endpoint(_) },
      rs.bytes("ipv4t") map { new Endpoint(_) },
      rs.bytes("ipv6u") map { new Endpoint(_) },
      rs.bytes("ipv4t") map { new Endpoint(_) },
      new Integer160(rs.getBytes("bucket")),
      rs.timestamp("replied"),
      rs.timestamp("queried"),
      rs.getInt("failcount"),
      this.timeout,
      this.threshold)
  }

  /**
   * Updates existing node by setting new values from the given [[org.abovobo.dht.Node]]
   * instance while leaving old values for those fields which are None in given node.
   * This method also updates a time stamp of bucket which owns a node.
   *
   * @param node    An instance of [[org.abovobo.dht.Node]] which has just been seen in network.
   * @param pn      Corresponding intstance of [[PersistentNode]].
   * @param method  Actual method how network node acted with us.
   */
  private def update(node: Node, pn: PersistentNode, method: Method): Unit = {
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
    s.executeUpdate()

    this.touch(pn.bucket)
  }

  /**
   * Attempts to insert new node into this table.
   *
   * @param node    An instance of [[org.abovobo.dht.Node]] to insert
   * @param method  A method of receiving network message from node.
   * @return        Result of operation, as listed in [[org.abovobo.dht.RoutingTable.Result.Result]]
   *                excluding [[org.abovobo.dht.RoutingTable.Result.Result#Updated]]
   */
  private def insert(node: Node, method: Method): Result = {

    import context.dispatcher
    import org.abovobo.jdbc.Closer._

    // read all buckets from storage
    val buckets = new mutable.ArrayBuffer[Integer160]()
    using(this.statements.allBuckets.executeQuery()) { rs =>
      while (rs.next()) {
        buckets += new Integer160(rs.getBytes(1))
      }
    }

    // get the bucket which is good for the given node
    val bucket = if (buckets.isEmpty) {
      // if there are no buckets exist we must insert zeroth bucket
      val zero = Integer160.zero
      this.statements.insertBucket.setBytes(1, zero.toArray)
      this.statements.insertBucket.executeUpdate()
      zero -> Integer160.maxval
    } else {
      // since zeroth element of this collection must be Integer160.zero
      // this always leads to valid index: we never can get -1 here.
      // so here we are getting last bucket having min bound less or equal to node id.
      val index = buckets.lastIndexWhere(_ <= node.id)
      buckets(index) -> (if (index == buckets.length - 1) Integer160.maxval else buckets(index + 1))
    }

    // read all nodes from that bucket
    val nodes = new mutable.ArrayBuffer[PersistentNode]()
    this.statements.nodesByBucket.setBytes(1, bucket._1.toArray)
    using(this.statements.nodesByBucket.executeQuery()) { rs =>
      while (rs.next()) {
        nodes += this.read(rs)
      }
    }

    // go through variants
    if (nodes.size == this.K) {
      // bucket is full
      // get list of good nodes in this bucket
      val good = nodes.filter(_.good)
      if (good.size == this.K) {
        // the bucket is full of good nodes
        // check if this id falls into a bucket range
        if (bucket._1 <= this.id && this.id < bucket._2) {
          // check if current bucket is large enough to be split
          if (bucket._2 - bucket._1 <= this.K * 2) {
            // current bucket is too small
            Rejected
          } else {
            // split current bucket and send the message back to self queue
            this.split(nodes, bucket)
            self ! (if (method == Reply) GotReply(node) else GotQuery(node))
            Deferred
          }
        } else {
          // own id is outside the bucket, so no more nodes can be inserted
          Rejected
        }
      } else {
        // not every node in this bucket is good
        // get the list of bad nodes
        val bad = nodes.filter(_.bad)
        if (bad.size > 0) {
          // there are bad nodes, replacing first of them
          this.statements.deleteNode.setBytes(1, bad.head.id.toArray)
          this.statements.deleteNode.executeUpdate
          this.insert(node, method, bucket._1)
          Replaced
        } else {
          // there are no bad nodes in this bucket
          // get list of questionnable nodes
          val questionnable = nodes.filter(_.questionnable)
          // request ping operation for every questionnable node
          questionnable foreach { sender ! RequestPing(_) }
          // TODO Request must not be sent to sender but rather to Networking Actor
          // send deferred message to itself
          this.context.system.scheduler.scheduleOnce(this.delay) {
            self ! (if (method == Reply) GotReply(node) else GotQuery(node))
          }
          Deferred
        }
      }
    } else {
      // there is a room for new node in this bucket
      this.insert(node, method, bucket._1)
      Inserted
    }
  }

  /**
   * This method splits given bucket onto 2 equal buckets moving nodes belonging
   * to the new bucket into it.
   *
   * @param nodes   Collection of nodes belonging to exsting bucket.
   * @param bucket  Existing bucket.
   */
  private def split(nodes: Traversable[Node], bucket: (Integer160, Integer160)): Unit = {
    // new bucket edge must split existing buckets onto 2 equals buckets
    val b = bucket._1 + ((bucket._2 - bucket._1) >> 1)
    // insert new bucket
    this.statements.insertBucket.setBytes(1, b.toArray)
    this.statements.insertBucket.executeUpdate()
    // move nodes appropriately
    nodes.filter(_.id >= b).foreach { node =>
      this.statements.moveNode.setBytes(1, b.toArray)
      this.statements.moveNode.setBytes(2, node.id.toArray)
      this.statements.moveNode.executeUpdate()
    }
  }

  /**
   * Actually executes insertion of a node into a storage also updating time stamp of owning bucket.
   *
   * @param node    A node being inserted.
   * @param method  A method of receiving network message from node.
   * @param bucket  A bucket which owns new node.
   */
  private def insert(node: Node, method: Method, bucket: Integer160): Unit = {

    // insert into node(id, bucket, ipv4u, ipv4t, ipv6u, ipv6t, replied, queried) values(?, ?, ?, ?, ?, ?, ?, ?)
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

    this.touch(bucket)
  }

  /// Initializes sibling `agent` actor reference
  private lazy val agent = this.context.actorSelection("../agent")

  /// Initializes actual connection to H2 database in a lazy way
  private lazy val connection = {
    Class.forName("org.h2.Driver")
    DriverManager.getConnection("jdbc:h2:" + db + ";AUTOCOMMIT=OFF")
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
    lazy val updateNode = c.prepareStatement(
      "update node set ipv4u=?, ipv4t=?, ipv6u=?, ipv6t=?, replied=?, queried=?, failcount=? where id=?")
    lazy val moveNode = c.prepareStatement("update node set bucket=? where id=?")
    lazy val deleteNode = c.prepareStatement("delete from node where id=?")
    lazy val deleteAllNodes = c.prepareStatement("delete from node")

    // Bucket related queries
    lazy val allBuckets = c.prepareStatement("select * from bucket order by id")
    lazy val insertBucket = c.prepareStatement("insert into bucket(id, seen) values(?, now())")
    lazy val touchBucket = c.prepareStatement("update bucket set seen=now() where id=?")
    lazy val deleteAllBuckets = c.prepareStatement("delete from bucket")

    lazy val setId = c.prepareStatement("insert into self(id) values(?)")
    lazy val dropId = c.prepareStatement("delete from self")
    lazy val getId = c.prepareStatement("select * from self")

    lazy val all = Array(nodeById, nodesByBucket, insertNode, updateNode, moveNode, deleteNode, deleteAllNodes,
      allBuckets, insertBucket, touchBucket, deleteAllBuckets,
      setId, dropId, getId)
  }

  /// This node SHA-1 identifier.
  /// This is variable which can be replaced by the one read from the storage
  /// or even set externally
  private var id: Integer160 = Integer160.random

  /// Collection of cancellable deferred tasks for refreshing buckets
  private val cancellables: mutable.Map[Integer160, Cancellable] = mutable.Map.empty
}

/** Accompanying object */
object RoutingTable {

  /**
   * Factory which creates RoutingTable Actor Props instance.
   *
   * @param K         Max number of entries per bucket.
   * @param timeout   Time interval before node or bucket becomes questionable.
   *                  In documentation above is 15 minutes.
   * @param delay     A delay before deferred message must be redelivered to self.
   *                  This duration must normally be a bit longer when waiting node
   *                  reply timeout.
   * @param threshold Number of times a node must fail to respond before being marked as 'bad'.
   * @param path      File system path to database where routing table state is persisted.
   *
   * @return          Properly configured Actor Props instance.
   */
  def props(K: Int, timeout: Duration, delay: Duration, threshold: Int, path: String): Props =
    Props(classOf[RoutingTable], K, timeout, delay, threshold, path)

  /**
   * Factory which creates RoutingTable Actor Props instance with default values:
   * K = 8, timeout = 15 minutes, delay = 30 seconds, threshold = 3.
   *
   * @param path      File system path to database where routing table state is persisted.
   *
   * @return          Properly configured Actor Props instance.
   */
  def props(path: String): Props = this.props(8, 15.minutes, 30.seconds, 3, path)

  /**
   * Factory which creates RoutingTable Actor Props instance with default values:
   * K = 8, timeout = 15 minutes, delay = 30 seconds, threshold = 3, path = ~/db/dht.
   *
   * @return          Properly configured Actor Props instance.
   */
  def props(): Props = this.props("~/db/dht")

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
   * Basic trait for all messages supported by [[org.abovobo.dht.RoutingTable]] actor.
   */
  sealed trait Message

  /**
   * This class represents a case when network message has been received
   * from the given node.
   *
   * @param node A Node instance of the subject.
   * @param kind A kind of message received from the node.
   */
  case class GotMessage(node: Node, kind: network.Message.Kind.Kind) extends Message

  /**
   * Instructs routing table to generate new SHA-1 identifier.
   */
  case class Reset() extends Message

  /**
   * Instructs routing table to set given id as own identifier.
   *
   * @param id An id to set.
   */
  case class Set(id: Integer160) extends Message

  /**
   * Instructs routing table to cleanup all stored data.
   */
  case class Purge() extends Message

  /**
   * Instructs routing table to refresh bucket with given id.
   *
   * @param id An id of the bucket to refresh.
   */
  case class Refresh(id: Integer160) extends Message

  /**
   * This class represents a message which can be sent by [[org.abovobo.dht.RoutingTable]]
   * back to a sender when pinging the questionnable node is needed.
   *
   * @param node A [[org.abovobo.dht.Node]] instance of the subject.
   */
  case class RequestPing(node: Node) extends Message

  /**
   * This class represents a message which is sent by [[org.abovobo.dht.RoutingTable]]
   * back to sender reporting incoming message results processing.
   *
   * @param result A tuple containing original [[org.abovobo.dht.Node]] and
   *               [[org.abovobo.dht.RoutingTable.Result.Result]] enumeration
   *               indicating processing outcome.
   */
  case class Report(result: (Node, Result.Result)) extends Message

  /**
   * Represents a message sent by [[org.abovobo.dht.RoutingTable]] back to sender
   * telling new id set by means of ResetId or SetId messages
   *
   * @param id A new table identifier
   */
  case class Done(id: Integer160) extends Message
}

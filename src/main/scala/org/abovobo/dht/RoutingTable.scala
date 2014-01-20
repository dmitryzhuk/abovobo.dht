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
import akka.actor.{ActorLogging, Cancellable, Props, Actor}
import scala.concurrent.duration._
import scala.collection.mutable
import org.abovobo.dht.persistence.{Writer, Reader, PersistentNode}

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
 * @param reader    Instance of [[org.abovobo.dht.persistence.Reader]] used to access persisted data.
 * @param writer    Instance of [[org.abovobo.dht.persistence.Writer]] to update persisted DHT state.
 *
 * @author Dmitry Zhuk
 */
class RoutingTable(val K: Int,
                   val timeout: FiniteDuration,
                   val delay: FiniteDuration,
                   val threshold: Int,
                   val reader: Reader,
                   val writer: Writer) extends Actor with ActorLogging {

  import RoutingTable._
  import RoutingTable.Result._
  import Controller._

  /**
   * @inheritdoc
   *
   * Handles RoutingTable Actor specific messages.
   */
  override def receive = {
    case Received(node, kind) => this.process(node, kind)
    case Refresh(min, max)    => this.refresh(min, max)
    case Reset()              => this.reset()
    case Set(id)              => this.set(id)
    case Purge()              => this.purge()
  }

  /**
   * @inheritdoc
   *
   * This override checks if there is a stored node id and generates new one if not.
   */
  override def preStart() {

    // check if the table already has assigned ID and reset if not
    // in any case initial FindNode will be issued to controller
    this.reader.id() match {
      case None     => this.reset()
      case Some(id) => this.controller ! FindNode(id)

    }

    import this.context.system

    // upon start also perform refresh procedure for every existing bucket
    // and schedule the next refresh after configured idle timeout
    //implicit val ec = this.context.system.dispatcher
    var prev: Integer160 = null
    this.reader.buckets() foreach { bucket =>
      if (prev ne null) {
        self ! Refresh(prev, bucket._1)
        this.cancellables.put(prev,
          system.scheduler.scheduleOnce(this.timeout)(self ! Refresh(prev, bucket._1))(system.dispatcher))
      }
      prev = bucket._1
    }
    if (prev eq null) prev = Integer160.zero
    self ! Refresh(prev, Integer160.maxval)
    this.cancellables.put(prev,
      system.scheduler.scheduleOnce(this.timeout)(self ! Refresh(prev, Integer160.maxval))(system.dispatcher))
  }

  /**
   * @inheritdoc
   *
   * Cancels all scheduled tasks.
   */
  override def postStop() {
    this.cancellables.foreach(_._2.cancel())
    this.cancellables.clear()
  }

  /**
   * This method processes an event of receiving network message from remote node
   * considering given kind of network message from that node. If given node does not
   * exist in this routing table there will be attempt to insert it into a table made,
   * otherwise, node data will be updated. If node is being inserted the buckets may be
   * split or node insertion still can be rejected if there was no room for the new node
   * in the table.
   *
   * @param node    A node to process network message from.
   * @param kind    A kind of network message received from the node.
   */
  def process(node: Node, kind: network.Message.Kind.Kind): Result = {

    import network.Message.Kind._

    this.log.info(
      "Processing incoming message with node id {} and kind {} received from {}",
      node.id, kind, sender)

    this.reader.node(node.id) match {
      case None => if (kind == Query || kind == Reply) {
        // insert new node only if event does not indicate error or failure
        val result = this.insert(node, kind)
        this.log.info("Attempted insertion with result {}", result)
        result
      } else {
        // otherwise reject node
        this.log.info("Rejected processing")
        Rejected
      }
      case Some(pn) =>
        // update existing node
        this.writer.update(node, pn, kind)
        // touch owning bucket
        this.touch(pn.bucket, this.reader.next(pn.bucket))
        // respond with Updated Result
        this.log.info("Updated existing node")
        Updated
    }
  }

  /**
   * This method initiates bucket refresh sequence by choosing
   * random number from within the bucket range and sending `find_node`
   * message to network agent actor.
   *
   * @param min Lower bound of bucket
   * @param max Upper bound of bucket
   */
  def refresh(min: Integer160, max: Integer160): Unit = {
    this.controller ! FindNode(min + Integer160.random % (max - min))
    // cancel existing bucket task if exists
    this.cancellables.get(min) foreach { _.cancel() }
    this.cancellables.remove(min)
    // schedule new refresh bucket task
    this.cancellables.put(
      min,
      this.context.system.scheduler.scheduleOnce
        (this.timeout)(self ! Refresh(min, this.reader.next(min)))(this.context.system.dispatcher)
    )
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
  private def set(id: Integer160): Unit = this.writer.transaction {
    this.cancellables.foreach(_._2.cancel())
    this.cancellables.clear()
    this.writer.drop()
    this.writer.id(id)
    this.controller ! FindNode(id)
  }

  /**
   * Deletes all data from database.
   */
  def purge(): Unit = this.writer.transaction {
    this.cancellables.foreach(_._2.cancel())
    this.cancellables.clear()
    this.writer.drop()
  }

  /**
   * Attempts to insert a new node into this table. The whole method is wrapped into
   * [[org.abovobo.dht.persistence.Writer#transaction]] block.
   *
   * @param node    An instance of [[org.abovobo.dht.Node]] to insert
   * @param kind    A kind network message received from node.
   * @return        Result of operation, as listed in [[org.abovobo.dht.RoutingTable.Result]]
   *                excluding `Updated` value.
   */
  private def insert(node: Node, kind: network.Message.Kind.Kind): Result = this.writer.transaction {

    val buckets = this.reader.buckets().toArray

    // get the bucket which is good for the given node
    val bucket = if (buckets.isEmpty) {
      // if there are no buckets exist we must insert zeroth bucket
      val zero = Integer160.zero
      this.writer.insert(zero)
      zero -> Integer160.maxval
    } else {
      // since zeroth element of this collection must be Integer160.zero
      // this always leads to valid index: we never can get -1 here.
      // so here we are getting last bucket having min bound less or equal to node id.
      val index = buckets.lastIndexWhere(_._1 <= node.id)
      buckets(index)._1 -> (if (index == buckets.length - 1) Integer160.maxval else buckets(index + 1)._1)
    }

    val nodes = this.reader.bucket(bucket._1) map { new LiveNode(_, this.timeout, this.threshold) }
    val id = this.reader.id().get

    // go through variants
    if (nodes.size == this.K) {
      // bucket is full
      // get list of good nodes in this bucket
      val good = nodes.filter(_.good)
      if (good.size == this.K) {
        // the bucket is full of good nodes
        // check if this id falls into a bucket range
        if (bucket._1 <= id && id < bucket._2) {
          // check if current bucket is large enough to be split
          if (bucket._2 - bucket._1 <= this.K * 2) {
            // current bucket is too small
            Rejected
          } else {
            // split current bucket and send the message back to self queue
            // new bucket edge must split existing buckets onto 2 equals buckets
            val b = bucket._1 + ((bucket._2 - bucket._1) >> 1)
            // insert new bucket
            this.writer.insert(b)
            // move nodes appropriately
            nodes.filter(_.node.id >= b) foreach { node => this.writer.move(node.node, b) }
            // send message to itself
            self ! Received(node, kind)
            // notify caller that insertion has been deferred
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
          this.writer.delete(bad.head.node.id)
          this.writer.insert(node, bucket._1, kind)
          this.writer.touch(bucket._1)
          Replaced
        } else {
          // there are no bad nodes in this bucket
          // get list of questionnable nodes
          val questionnable = nodes.filter(_.questionnable)
          // request ping operation for every questionnable node
          questionnable foreach { live => this.controller ! Ping(live.node.address) }
          // send deferred message to itself
          this.context.system.scheduler.scheduleOnce(this.delay)(self ! Received(node, kind))(this.context.dispatcher)
          // notify caller that insertion has been deferred
          Deferred
        }
      }
    } else {
      // there is a room for new node in this bucket
      this.writer.insert(node, bucket._1, kind)
      this.touch(bucket._1, bucket._2)
      Inserted
    }
  }

  /**
   * Touches a bucket with given id resetting its last seen time stamp and scheduling
   * new `Refresh` command.
   *
   * @param bucket A bucket to touch.
   * @param next   An id of the next bucket.
   */
  private def touch(bucket: Integer160, next: Integer160) = this.writer.transaction {
    import this.context.system
    // actually update bucket last seen property in storage
    this.writer.touch(bucket)
    // cancel existing bucket task if exists
    this.cancellables.get(bucket) foreach { _.cancel() }
    this.cancellables.remove(bucket)
    // schedule new refresh bucket task
    this.cancellables.put(bucket,
      system.scheduler.scheduleOnce(this.timeout)(self ! Refresh(bucket, next))(system.dispatcher))
  }

  /// Initializes sibling `controller` actor reference
  private lazy val controller = this.context.actorSelection("../controller")

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
   * @param reader    Instance of [[org.abovobo.dht.persistence.Reader]] used to access persisted data.
   * @param writer    Instance of [[org.abovobo.dht.persistence.Writer]] to update persisted DHT state.
   *
   * @return          Properly configured Actor Props instance.
   */
  def props(K: Int, timeout: Duration, delay: Duration, threshold: Int, reader: Reader, writer: Writer): Props =
    Props(classOf[RoutingTable], K, timeout, delay, threshold, reader, writer)

  /**
   * Factory which creates RoutingTable Actor Props instance with default values:
   * K = 8, timeout = 15 minutes, delay = 30 seconds, threshold = 3.
   *
   * @param reader    Instance of [[org.abovobo.dht.persistence.Reader]] used to access persisted data.
   * @param writer    Instance of [[org.abovobo.dht.persistence.Writer]] to update persisted DHT state.
   *
   * @return          Properly configured Actor Props instance.
   */
  def props(reader: Reader, writer: Writer): Props = this.props(8, 15.minutes, 30.seconds, 3, reader, writer)

  /**
   * Wraps [[org.abovobo.dht.persistence.PersistentNode]] with utility methods allowing
   * to obtain actual state of the node in terms of its activity.
   *
   * @param node      An instance of [[org.abovobo.dht.persistence.PersistentNode]].
   * @param timeout   Time interval before node becomes questionable.
   * @param threshold Number of times a node must fail to respond to become 'bad'.
   */
  class LiveNode(val node: PersistentNode, val timeout: Duration, val threshold: Int) {

    /** Returns true if this node is "good" */
    def good: Boolean = !this.bad && !this.questionnable

    /** Returns true if this node is definitely bad */
    def bad: Boolean = this.threshold <= this.node.failcount

    /** Returns true if this node is not "bad" but has not been recently seen */
    def questionnable: Boolean =
      this.threshold > this.node.failcount && this.node.replied.isDefined && !this.recentlySeen

    /** Returns true if this node has been seen recently */
    def recentlySeen: Boolean =
      this.node.replied.map(d => (System.currentTimeMillis - d.getTime).milliseconds).getOrElse(Duration.Inf) < this.timeout ||
        this.node.queried.map(d => (System.currentTimeMillis - d.getTime).milliseconds).getOrElse(Duration.Inf) < this.timeout
  }

  /**
   * This enumeration defines four possible outcomes of touching the node:
   *
   * 1. Inserted for new node, Updated if node already existed in the table.
   * 2. Replaced for the case when new node replaced existing bad node.
   * 3. Rejected for the new node when there was no room in the table for it.
   * 4. Deferred for the case when node processing has been deffered until some
   *    checks with existing questionable nodes are done.
   */
  object Result extends Enumeration {
    type Result = Value
    val Inserted, Replaced, Updated, Rejected, Deferred = Value
  }

  /**
   * Basic trait for all commands supported by [[org.abovobo.dht.RoutingTable]] actor.
   */
  sealed trait Command

  /**
   * This class represents a case when network message has been received
   * from the given node.
   *
   * @param from A Node from which a network message has been received.
   * @param kind A kind of message received from the node.
   */
  case class Received(from: Node, kind: network.Message.Kind.Kind) extends Command

  /**
   * Instructs routing table to generate new random SHA-1 identifier.
   */
  case class Reset() extends Command

  /**
   * Instructs routing table to set given id as own identifier.
   *
   * @param id An id to set.
   */
  case class Set(id: Integer160) extends Command

  /**
   * Instructs routing table to cleanup all stored data.
   */
  case class Purge() extends Command

  /**
   * Instructs routing table to refresh the bucket with given bounds.
   *
   * @param min Lower bound of bucket.
   * @param max Upper bound of bucket.
   */
  case class Refresh(min: Integer160, max: Integer160) extends Command
}

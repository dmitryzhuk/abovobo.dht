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

import scala.concurrent.duration._
import scala.collection.mutable

import akka.actor.{ActorLogging, Cancellable, Props, Actor, ActorRef}
import org.abovobo.dht.message.Message
import org.abovobo.integer.Integer160
import org.abovobo.dht.persistence.Storage

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
 * @constructor      Creates new instance of routing table with provided parameters.
 *
 * @param K          Max number of entries per bucket.
 * @param timeout    Time interval before bucket becomes inactive or node becomes questionable.
 *                   In documentation above it is 15 minutes.
 * @param delay      A delay before deferred message must be redelivered to self.
 *                   This duration must normally be a bit longer when waiting node
 *                   reply timeout.
 * @param threshold  Number of times a node must fail to respond before being marked as 'bad'.
 * @param storage    Instance of [[org.abovobo.dht.persistence.Storage]] used to access persisted data.
 *
 * @author Dmitry Zhuk
 */
class Table(val K: Int,
            val timeout: FiniteDuration,
            val delay: FiniteDuration,
            val threshold: Int,
            val storage: Storage)
  extends Actor with ActorLogging {

  import this.context.system
  import this.context.dispatcher

  override def preStart() = {}

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
   * Defines initial event handler, which only handles [[Requester.Ready]] event.
   * As soon, as this event has occurred, Table switches event loop to [[Table.working()]]
   * function and initiates recursive lookup procedures as required by DHT specification.
   */
  def waiting: Receive = {
    case Requester.Ready =>
      // switch event loop to the one which is able to actually handle events
      this.context.become(this.working(this.sender()))
      // check if the table already has assigned ID and reset if not
      if (this.storage.id().isEmpty) this.reset(this.sender())

      // upon start also perform refresh procedure for every idle bucket
      // and schedule refresh for other buckets
      this.storage.buckets() foreach { bucket =>
        this.timeout.toMillis
        if (System.currentTimeMillis - bucket.seen.getTime >= this.timeout.toMillis) {
          self ! Table.Refresh(bucket)
        } else {
          this.cancellables.put(
            bucket.start,
            system.scheduler.scheduleOnce(
              (System.currentTimeMillis - bucket.seen.getTime).milli,
              self,
              Table.Refresh(bucket)))
        }
      }
    // To debug crashes
    case t: Throwable => throw t
  }

  /**
   * Defines general event handler.
   *
   * @param controller A reference to [[Requester]] actor.
   */
  def working(controller: ActorRef): Receive = {
    case Table.Refresh(bucket)      =>          this.refresh(bucket, controller)
    case Table.Reset()              => sender ! this.reset(controller)
    case Table.Set(id)              => sender ! this.set(id, controller)
    case Table.Purge()              => sender ! this.purge()
    case Table.Received(node, kind) => sender ! this.process(node, kind, controller)
    case Table.Failed(node)         => sender ! this.process(node, Message.Kind.Fail, controller)
    // To debug crashes
    case t: Throwable => throw t
  }

  /** @inheritdoc */
  override def receive = this.waiting

  /**
   * This method initiates bucket refresh sequence by choosing
   * random number from within the bucket range and sending `find_node`
   * message to network agent actor.
   *
   * @param bucket The bucket to refresh
   * @param controller  Reference to [[Requester]] actor.
   */
  private def refresh(bucket: Bucket, controller: ActorRef): Unit = {
    // request refreshing `find_node` by means of `Requester`
    controller ! Requester.FindNode(bucket.random)
    // cancel existing bucket task if exists
    this.cancellables.remove(bucket.start) foreach { _.cancel() }
    // schedule new refresh bucket task
    this.cancellables.put(
      bucket.start,
      system.scheduler.scheduleOnce(
        this.timeout,
        self,
        Table.Refresh(bucket)))
  }

  /**
   * Generates new SHA-1 node id, drops all data and saves new id.
   *
   * @param controller  Reference to [[Requester]] actor.
   */
  private def reset(controller: ActorRef): Table.Result = this.set(Integer160.random, controller)

  /**
   * Drops all data and saves new id in the storage.
   *
   * @param id New SHA-1 node identifier.
   * @param controller  Reference to [[Requester]] actor.
   */
  private def set(id: Integer160, controller: ActorRef): Table.Result = this.storage.transaction {
    this.cancellables.foreach(_._2.cancel())
    this.cancellables.clear()
    this.storage.drop()
    this.storage.id(id)
    controller ! Requester.FindNode(id)
    Table.Id(id)
  }

  /**
   * Deletes all data from database.
   */
  private def purge(): Table.Result = this.storage.transaction {
    this.cancellables.foreach(_._2.cancel())
    this.cancellables.clear()
    this.storage.drop()
    Table.Purged
  }

  /**
   * This method processes an event of receiving network message from remote node
   * considering given kind of network message from that node. If given node does not
   * exist in this routing table there will be attempt to insert it into a table made,
   * otherwise, node data will be updated. If node is being inserted the buckets may be
   * split or node insertion still can be rejected if there was no room for the new node
   * in the table.
   *
   * @param node        A node to process network message from.
   * @param kind        A kind of network message received from the node.
   * @param controller  Reference to [[Requester]] actor.
   */
  private def process(node: NodeInfo, kind: Message.Kind.Kind, controller: ActorRef): Table.Result = {

    import Message.Kind

    this.log.debug(
      "Processing incoming message with node id {} and kind {} received from {}",
      node.id, kind, this.sender())

    // get bucket which is suitable for this node
    val bucket = this.storage.bucket(node.id)

    this.storage.node(node.id) match {
      case None =>
        if (kind == Kind.Query || kind == Kind.Response) {
          // insert new node only if event does not indicate error or failure
          val result = this.insert(node, bucket, kind, controller)
          this.log.debug("Attempted insertion with result {}", result)
          result
        } else {
          // otherwise reject node
          this.log.debug("Rejected processing")
          Table.Rejected
        }
      case Some(pn) =>
        // update existing node
        this.storage.transaction {
          this.storage.update(node, pn, kind)
          this.touch(bucket)
        }
        Table.Updated
    }
  }

  /**
   * Attempts to insert a new node into this table. The whole method is wrapped into
   * [[org.abovobo.dht.persistence.Writer#transaction]] block.
   *
   * @param node        An instance of [[org.abovobo.dht.NodeInfo]] to insert
   * @param bucket      An instance of bucket which is good for the given node
   * @param kind        A kind network message received from node.
   * @param controller  Reference to [[Requester]] actor.
   * @return            Result of operation, as listed in [[org.abovobo.dht.Table.Result]]
   *                    excluding `Updated` value.
   */
  private def insert(node: NodeInfo,
                     bucket: Bucket,
                     kind: Message.Kind.Kind,
                     controller: ActorRef): Table.Result = this.storage.transaction {

    // get this node identifier
    val id = this.storage.id().get
    // get nodes from that bucket
    val nodes = this.storage.nodes(bucket)

    implicit val timeout = this.timeout
    implicit val threshold = this.threshold

    // go through variants
    if (nodes.size == this.K) {
      // bucket is full
      // get list of good nodes in this bucket
      val good = nodes.filter(_.good)
      if (good.size == this.K) {
        // the bucket is full of good nodes
        // check if this id falls into a bucket range
        if (id @: bucket) {
          // check if current bucket is large enough to be split
          if (bucket.length < this.K * 2) {
            // current bucket is too small
            Table.Rejected
          } else {
            // split current bucket and send the message back to self queue
            // new bucket edge must split existing buckets onto 2 equals buckets
            val b = bucket.mid
            // insert new bucket
            this.storage.insert(b)
            // send message to itself preserving original sender
            self.!(Table.Received(node, kind))(this.sender())
            // notify caller that insertion has been deferred
            Table.Split(bucket.start, b)
          }
        } else {
          // own id is outside the bucket, so no more nodes can be inserted
          // TODO Handle highly unbalanced trees
          Table.Rejected
        }
      } else {
        // not every node in this bucket is good
        // get the list of bad nodes
        val bad = nodes.filter(_.bad)
        if (bad.size > 0) {
          // there are bad nodes, replacing first of them
          this.storage.delete(bad.head.id)
          this.storage.insert(node, kind)
          this.storage.touch(bucket.start)
          Table.Replaced(bad.head)
        } else {
          // there are no bad nodes in this bucket
          // get list of questionable nodes
          val questionable = nodes.filter(_.questionable)
          // request ping operation for every questionable node
          questionable foreach { node => controller ! Requester.Ping(node) }
          // send deferred message to itself
          system.scheduler.scheduleOnce(
            this.delay, self, Table.Received(node, kind))(this.context.dispatcher, this.sender())
          // notify caller that insertion has been deferred
          Table.Deferred
        }
      }
    } else {
      // there is a room for new node in this bucket
      this.storage.insert(node, kind)
      this.touch(bucket)
      Table.Inserted(bucket.start)
    }
  }

  /**
   * Touches a bucket with given id resetting its last seen time stamp and scheduling
   * new `Refresh` command.
   *
   * @param bucket A bucket to touch.
   */
  private def touch(bucket: Bucket) = {
    // actually update bucket last seen property in storage
    this.storage.touch(bucket.start)
    // cancel existing bucket task if exists
    this.cancellables.remove(bucket.start) foreach { _.cancel() }
    // schedule new refresh bucket task
    this.cancellables.put(bucket.start, system.scheduler.scheduleOnce(this.timeout, self, Table.Refresh(bucket)))
  }

  /// Collection of cancellable deferred tasks for refreshing buckets
  private val cancellables: mutable.Map[Integer160, Cancellable] = mutable.Map.empty
}

/** Accompanying object */
object Table {

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
   * @param storage    Instance of [[org.abovobo.dht.persistence.Storage]] used to access persisted data.
   *
   * @return          Properly configured Actor Props instance.
   */
  def props(K: Int, timeout: FiniteDuration, delay: FiniteDuration, threshold: Int, storage: Storage): Props =
    Props(classOf[Table], K, timeout, delay, threshold, storage)

  /**
   * Factory which creates RoutingTable Actor Props instance with default values:
   * K = 8, timeout = 15 minutes, delay = 30 seconds, threshold = 3.
   *
   * @param storage    Instance of [[org.abovobo.dht.persistence.Storage]] used to access persisted data.
   *
   * @return          Properly configured Actor Props instance.
   */
  def props(storage: Storage): Props = this.props(8, 15.minutes, 30.seconds, 3, storage)

  /**
   * Basic trait for all possible outcomes of Table operation.
   *
   * Concrete case classess extending this trait will be sent back to sender
   * when the result of processing the command or handling the event is
   * known.
   */
  sealed trait Result

  /**
   * Means that some bucket was split while handling [[org.abovobo.dht.Table.Received]] event.
   * It may be sent multiple times during processing single event.
   *
   * @param was Lower bound of the bucket which has been split.
   * @param now Lower bound of the new bucket after split.
   */
  case class Split(was: Integer160, now: Integer160) extends Result

  /**
   * Indicates that new NodeInfo has been inserted into a table.
   *
   * @param bucket Lower bound of the bucket in which new NodeInfo has been inserted.
   */
  case class Inserted(bucket: Integer160) extends Result

  /**
   * Indicates that existing (bad) NodeInfo has been replaced with new NodeInfo.
   *
   * @param old An old node instance which has been replaced with the new one.
   */
  case class Replaced(old: KnownNodeInfo) extends Result

  /** Indicates that NodeInfo has already been in the table, so its info has just been updated. */
  case object Updated extends Result

  /** Indicates that there was no room for the new NodeInfo in the table. */
  case object Rejected extends Result

  /**
   * Indicates that further processing of the node has been deferred until some additional
   * information is received. This normally happens when there are questionnable nodes
   * and table must check if they are good or bad before deciding what to do next.
   */
  case object Deferred extends Result

  /** Successfull result of processing [[org.abovobo.dht.Table.Purge]] command. */
  case object Purged extends Result

  /**
   * Sent back to sender of [[org.abovobo.dht.Table.Set]] or [[org.abovobo.dht.Table.Reset]]
   * commands.
   *
   * @param id An identifier of this table after `Set` or `Reset` command is executed.
   */
  case class Id(id: Integer160) extends Result

  /** Basic trait for all events fired by other actors which can be handled by Table. */
  sealed trait Event

  /**
   * This class represents a case when network message has been received
   * from the given node.
   *
   * @param node A NodeInfo from which a network message has been received.
   * @param kind A kind of message received from the node.
   */
  case class Received(node: NodeInfo, kind: Message.Kind.Kind) extends Event

  /**
   * This class represents a case when remote peer represented by node has failed
   * to respond to our request in timely manner.
   *
   * @param node A NodeInfo which has failed to respond to our query.
   */
  case class Failed(node: NodeInfo) extends Event

  /** Basic trait for all commands supported by [[org.abovobo.dht.Table]] actor. */
  sealed trait Command

  /** Instructs routing table to generate new random SHA-1 identifier. */
  case class Reset() extends Command

  /**
   * Instructs routing table to set given id as own identifier.
   *
   * @param id An id to set.
   */
  case class Set(id: Integer160) extends Command

  /** Instructs routing table to cleanup all stored data. */
  case class Purge() extends Command

  /**
   * Instructs routing table to refresh the bucket with given bounds.
   *
   * @param bucket A bucket to refresh.
   */
  case class Refresh(bucket: Bucket) extends Command
}

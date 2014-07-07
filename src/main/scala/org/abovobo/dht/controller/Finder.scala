/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.controller

import org.abovobo.dht._
import org.abovobo.integer.Integer160

import scala.collection.mutable

/**
 * This class collects data during recursive `find_node` or `get_peers` operations.
 * Instance of this class is completely reactive and single-threaded.
 *
 * From the original Kademlia paper:
 *
 *  "The most important procedure a Kademlia participant must perform is to
 *  locate the k closest nodes to some given node ID. We call this procedure a
 *  node lookup. Kademlia employs a recursive algorithm for node lookups. The
 *  lookup initiator starts by picking α nodes from its closest non-empty
 *  k-bucket (or, if that bucket has fewer than α entries, it just takes the α
 *  closest nodes it knows of). The initiator then sends parallel, asynchronous
 *  FIND_NODE RPCs to the α nodes it has chosen. α is a system-wide concurrency
 *  parameter, such as 3.
 *
 *  In the recursive step, the initiator resends the FIND_NODE to nodes it has
 *  learned about from previous RPCs. (This recursion can begin before all α of
 *  the previous RPCs have returned). Of the k nodes the initiator has heard of
 *  closest to the target, it picks α that it has not yet queried and resends
 *  the FIND NODE RPC to them. Nodes that fail to respond quickly are removed
 *  from consideration until and unless they do respond. If a round of FIND
 *  NODEs fails to return a node any closer than the closest already seen, the
 *  initiator resends the FIND NODE to all of the k closest nodes it has not
 *  already queried. The lookup terminates when the initiator has queried and
 *  gotten responses from the k closest nodes it has seen. When α = 1 the
 *  lookup algorithm resembles Chord’s in terms of message cost and the latency
 *  of detecting failed nodes. However, Kademlia can route for lower latency
 *  because it has the flexibility of choosing any one of k nodes to forward a
 *  request to."
 *
 * @constructor   Creates new instance of Finder.
 *
 * @param target  A target 160-bit integer against which the find procedure is being ran.
 * @param K       A size of K-bucket used to calculate current state of finder.
 * @param alpha   System-wide parameter alpha defining typical number of requests per round.
 * @param seeds   A collection of nodes to start with.
 */
class Finder(val target: Integer160, val K: Int, val alpha: Int, val seeds: Traversable[Node]) {

  /// Defines implicit [[math.Ordering]] for [[org.abovobo.dht.Node]] instances.
  private implicit val ordering = new NodeOrdering(this.target)

  /// Collection of all nodes which were seen by means of node information sent with responses.
  /// This collection is used to check if there were closer nodes learned from completed requests.
  private val _seen = new mutable.TreeSet[Node]

  /// Collection of nodes which were seen but not yet taken to be queried
  private val _untaken = new mutable.TreeSet[Node]

  /// Collection of pending request rounds
  private val _pending = new Rounds()

  /// Collection of completed request rounds
  private val _completed = new Rounds()

  /// Collection of nodes which reported successfully
  private val _succeeded = new mutable.TreeSet[Node]

  /// Collection of node id -> token associations
  private val _tokens = new mutable.HashMap[Integer160, Token]

  /// Collection of peers reported by queried nodes
  private val _peers = new mutable.HashSet[Peer]

  // Dump all seeds into `untaken` and `seen`
  this._seen ++= this.seeds
  this._untaken ++= this.seeds

  /**
   * Reports transaction completion bringing nodes, peers and token from response.
   *
   * @param reporter  A node which has sent a [[org.abovobo.dht.message.Response]].
   * @param nodes     A collection of nodes reported by queried node.
   * @param peers     A collection of peers reported by queried node.
   * @param token     A token distributed by queried node.
   */
  def report(reporter: Node, nodes: Traversable[Node], peers: Traversable[Peer], token: Token) = {

    // detect if reporting node will improve collection of seen nodes
    val improved = nodes.foldLeft(0) { (r, node) =>
      if (this.ordering.compare(node, this._seen.head) == -1) r + 1 else r
    }

    // update request result value
    this._pending.get(reporter) match {
      case Some((round, request)) =>
        request.result =
          if (improved > 0)
            Request.Improved(improved)
          else
            Request.Neutral()
      case None =>
        // Reporter was not found in collection of pending requests.
        // It probably means that request has already timed out.
        // XXX We might want to handle this case in the future
    }

    // if head round of the queue of pending request rounds
    // became completed after this report, move it to collection
    // of completed rounds for further reference
    while (this._pending.nonEmpty && (this._pending.front.result match {
      case Request.Neutral() | Request.Failed() => true
      case Request.Improved(n) => true
      case _ => false
    })) this._completed.enqueue(this._pending.dequeue())

    // store reporter into collection of succeeded nodes
    // but don't add routers (nodes with zero id)
    if (reporter.id != Integer160.zero) { 
      this._succeeded += reporter
    }
    
    // store node->token association
    if (token.nonEmpty) this._tokens += reporter.id -> token

    // add reported peers to internal collection
    this._peers ++= peers

    // add unseen reported nodes into a collection of seen and untaken nodes
    nodes.foreach { node =>
      if (!this._seen.contains(node)) {
        this._seen += node
        this._untaken += node
      }
    }
  }
  
  /**
   * Indicates that given node failed to respond in timely manner.
   *
   * @param node  A node which failed to respond in timely manner.
   */
  def fail(node: Node) = {
    this._pending.get(node) match {
      case Some((round, request)) =>
        request.result = Request.Failed()
      case None =>
        // Reporter was not found in collection of pending requests.
        // It probably means that request has already timed out.
        // This can only happen if remote peer responded with Error
        // message after timeout expired. Thus, this case can safely
        // be ignored.
    }
  }

  /**
   * Returns current state of [[Finder]] instance.
   */
  def state =

    // case #0: The beginning of Finder lifecycle: no nodes has been taken yet
    if (this._pending.isEmpty && this._succeeded.isEmpty && this._untaken.nonEmpty && this._seen.nonEmpty)
      Finder.State.Continue

    // case #1: No pending requests, no untaken nodes, no succeeded nodes means
    //          that node lookup attempt has totally failed.
    else if (this._pending.isEmpty && this._untaken.isEmpty && this._succeeded.size < this.K)
      Finder.State.Failed

    // case #2: Either currently pending request has improved collection of seen nodes or
    //          (if there are no pending requests) the most recent of completed ones did that.
    else if ((this._pending.nonEmpty && this._pending.front.improved > alpha) ||
      (this._pending.isEmpty && this._completed.nonEmpty && this._completed.last.improved > alpha))
      Finder.State.Continue

    // case #3: There are more than `K` succeeded responses AND: all `K` of succeeded nodes are
    //          closer than any of untaken ones OR there are no untaken nodes remaining.
    else if (this._pending.isEmpty && this._succeeded.size >= this.K &&
      (this._completed.nonEmpty && this._completed.last.improved == 0) &&
      (this.ordering.compare(this._succeeded.take(this.K).last, this._untaken.head) == -1 || this._untaken.isEmpty))
      Finder.State.Succeeded

    // case #4: Last round did not bring any nodes which are closer than already seen ones,
    //          means that initator should consider to send requests to K closest known nodes,
    //          which were not yet queried.
    else if (this._completed.nonEmpty && this._completed.last.improved == 0 && this._pending.isEmpty)
      Finder.State.Finalize

    // case #5: If none of the cases above but still there are pending requests
    //          just wait for more requests to complete.
    else if (this._pending.nonEmpty)
      Finder.State.Wait

    // Otherwise throw an exception. In theory this must never happen.
    else throw new RuntimeException("Invalid Finder State")

  /**
   * Takes maximum `n` nodes which were not given yet.
   *
   * @param n max number of nodes to give back.
   * @return  maximum `n` nodes which were not given yet.
   */
  def take(n: Int) = {
    val more = this._untaken.take(n)
    this._untaken --= more
    this._pending.enqueue(new Round(more.map(new Request(_))))
    more
  }

  /** Returns maxium K succeeded nodes */
  def nodes = this._succeeded.take(this.K)

  /**
   * Returns token ([[scala.Option]]) for given node id.
   *
   * @param id Node id to return token for.
   * @return   token for given node id.
   */
  def token(id: Integer160) = this._tokens.get(id)

  /** Returns map of all reported tokens */
  def tokens: collection.Map[Integer160, Token] = this._tokens

  /** Returns collection of all reported peers */
  def peers: collection.Traversable[Peer] = this._peers
}

/** Accompanying object */
object Finder {

  /** Defines enumeration of possible [[controller.Finder]] states */
  object State extends Enumeration {

    val Continue,   // The search recursion should continue:
                    // -------------------------------------
                    // "Of the K nodes the initiator has heard of closest to the target,
                    //  it picks α that it has not yet queried and resends the FIND_NODE
                    //  RPC to them. This recursion can begin before all α nodes of the
                    //  previous RPC has returned."

        Wait,       // In order to determine the following state the initiator must wait
                    // until one or more pending requests is completed.


        Finalize,   // The search recursion should be finalized:
                    // -----------------------------------------
                    // "If a round of FIND_NODEs fails to return a node any closer than
                    //  the closest already seen, the initiator resends the FIND_NODE to
                    //  all of the K closest nodes it has not already queried."

        Succeeded,  // Node lookup is finished with results:
                    // -------------------------------------
                    // "The lookup terminates when the initiator has queried and gotten
                    //  responses from the k closest nodes it has seen."

        Failed      // Node lookup is finished without results


    = Value
  }

}

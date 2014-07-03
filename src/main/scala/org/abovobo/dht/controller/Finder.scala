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
 * @param seeds   A collection of nodes to start with.
 */
class Finder(val target: Integer160, val K: Int, val seeds: Traversable[Node]) {

  /// Defines implicit [[math.Ordering]] for [[org.abovobo.dht.Node]] instances.
  private implicit val ordering = new NodeOrdering(this.target)

  /// Collection of all nodes which were seen by means of node information sent with responses
  private val _seen = new mutable.TreeSet[Node]

  /// Collection of pending request rounds; round is removed from the queue when its size reaches 0
  private val _pending = new mutable.Queue[Round]

  /// Collection of nodes which were seen but not yet taken to be queried
  private val _untaken = new mutable.TreeSet[Node]

  /// Collection of nodes which reported successfully
  private val _succeeded = new mutable.TreeSet[Node]

  /// Collection of node id -> token associations
  private val _tokens = new mutable.HashMap[Integer160, Token]

  /// Collection of peers reported by queried nodes
  private val _peers = new mutable.HashSet[Peer]

  // Dump all seeds into `untaken` and `seen`
  this.add(this.seeds)

  /**
   * Reports transaction completion bringing nodes, peers and token from response.
   *
   * @param reporter  A node which has sent a [[org.abovobo.dht.message.Response]].
   * @param nodes     A collection of nodes reported by queried node.
   * @param peers     A collection of peers reported by queried node.
   * @param token     A token distributed by queried node.
   */
  def report(reporter: Node, nodes: Traversable[Node], peers: Traversable[Peer], token: Token) = {
    // remove reporter from the collection of pending nodes
    // note that basically reporter may not be here at all
    this._pending -= reporter

    // don't add routers (nodes with zero id) into result
    if (reporter.id != Integer160.zero) { 
      // store reporter into collection of succeded nodes
      this._succeeded += reporter
    }
    
    // store node->token association
    if (token.nonEmpty) this._tokens += reporter.id -> token

    // add reported peers to internal collection
    this._peers ++= peers

    this.add(nodes)
  }
  
  /**
   * Indicates that given node failed to respond in timely manner.
   *
   * @param node  A node which failed to respond in timely manner.
   */
  def fail(node: Node) = {
    this._pending -= node
  }

  /**
   * Indicates current state of the Finder object:
   * -- if pending requests list is empty and no untaken nodes exist
   *      * Succeeded:  if there are succeeded nodes
   *      * Failed:     otherwise
   * -- if there are at least K succeeded nodes which are `closer` to `target` then closest untaken one
   *      * Succeeded:  if there are no pending requests
   *      * Pending:    if there are still pending requests remain
   * -- Continue in any other case
   *
   * XXX: FIXME: Update this logic, so it would wait for at least a round of pending requests, without this, we might often stop after first K responded nodes (on second round with alpha 3 and K 8, actually),
   *    thou there might be closer responses from pending requests.
   *    We can add "Pending" state which will not generate new requests nor finish recursion, until closer nodes are found or pending set is empty (or at least have less entries then current round 'width')
   * 
   */
  def state =
    if (this._pending.isEmpty && this._untaken.isEmpty) {
      if (this._succeeded.isEmpty)
        Finder.State.Failed
      else 
        Finder.State.Succeeded
    } else if (this._succeeded.size >= K
                && (this._untaken.isEmpty || this.ordering.lteq(this._succeeded.take(this.K).last, this._untaken.head))) {
                // XXX: 
                // DONE: a) fix the case when we kill requests with pending closer nodes, 
                // TODO: b) fix case for GetPeers, when our goal is to get peers not nodes

      // Means we've got some results, but there are unanswered nodes, with possibly better results. 
      // So we can wait for some timeout or some activity from them with better results
      Finder.State.Pending
    } else {
      Finder.State.Continue
    }

  /**
   * Takes maximum `n` nodes which were not given yet.
   *
   * @param n max number of nodes to give back.
   * @return  maximum `n` nodes which were not given yet.
   */
  def take(n: Int) = {
    val more = this._untaken.take(n)
    this._untaken --= more
    this._pending ++= more
    more
  }

  /** Returns maxium K succeeded nodes */
  def nodes = this._succeeded.take(this.K)

  /**
   * Returns token ([[scala.Option]]) for given node id.
   * @param id Node id to return token for.
   * @return   token for given node id.
   */
  def token(id: Integer160) = this._tokens.get(id)

  /** Returns map of tokens */
  def tokens: collection.Map[Integer160, Token] = this._tokens

  /** Returns collection of peers */
  def peers: collection.Traversable[Peer] = this._peers

  /**
   * Adds unseen nodes from the given collection into the collection of seen nodes and untaken nodes.
   *
   * @param nodes A collection of potentially unseen nodes.
   */
  private def add(nodes: Traversable[Node]) = nodes foreach { node =>
    if (!this._seen.contains(node)) {
      this._seen += node
      this._untaken += node
    }
  }

  /**
   * Removes given node from the corresponding round of pending requests.
   * If, as a result of this operation, round becomes empty, removes the round from the queue.
   *
   * @param node a node request to which was complete.
   */
  private def complete(node: Node) = {
    // --
  }
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

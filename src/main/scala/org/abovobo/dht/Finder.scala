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
import scala.collection.mutable
import java.net.InetSocketAddress
import java.lang.IllegalArgumentException

/**
 * This class collects data during recursive `find_node` or `get_peers` operations.
 *
 * @param target  A target 160-bit integer against which the find procedure is being ran.
 * @param K       A size of K-bucket used to calculate current state of finder.
 */
abstract class Finder(val target: Integer160, K: Int, seeds: Traversable[Node]) {
  
  /** 
   *  Defines implicit [[scala.math.Ordering]] for [[org.abovobo.dht.Node]] instances. 
   *
   *  XXX: we're loosing nodes which have same distance to the target but different IDs, but due to huge values domain such collisions shoudln't be a problem
   *  
   */
  implicit val ordering = new scala.math.Ordering[Node] {
    override def compare(x: Node, y: Node): Int = {
      val x1 = Finder.this.target ^ x.id
      val y1 = Finder.this.target ^ y.id
      if (x1 < y1) -1 else if (x1 > y1) 1 else 0
    }
  }

  /// Collection of all nodes which were seen by means of node information sent with responses
  private val seen = new mutable.TreeSet[Node]

  /// Collection of nodes which has been taken but not reported yet
  private val pending = new mutable.TreeSet[Node]

  /// Collection of nodes which were seen but not yet taken to be queried
  private val untaken = new mutable.TreeSet[Node]

  /// Collection of nodes which reported successfully
  private val succeeded = new mutable.TreeSet[Node]

  /// Collection of node id -> token associations
  private val _tokens = new mutable.HashMap[Integer160, Token]

  /// Collection of peers reported by queried nodes
  private val _peers = new mutable.HashSet[Peer]

  // Dump all seeds into untaken
  this.add(this.seeds)

  /**
   * Reports transaction completion bringing nodes, peers and token from response.
   *
   * @param reporter  A node which has sent a [[org.abovobo.dht.Response]].
   * @param nodes     A collection of nodes reported by queried node.
   * @param peers     A collection of peers reported by queried node.
   * @param token     A token distributed by queried node.
   */
  def report(reporter: Node, nodes: Traversable[Node], peers: Traversable[Peer], token: Token) = {
    // remove reporter from the collection of pending nodes
    // note that initially reporter may not be here at all
    this.pending -= reporter

    // don't add routers (nodes with zero id) into result
    if (reporter.id != Integer160.zero) { 
      // store reporter into collection of succeded nodes
      this.succeeded += reporter
    }
    
    // store node->token association
    if (!token.isEmpty) this._tokens += reporter.id -> token

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
    this.pending -= node
  }

  /**
   * Indicates current state of the Finder object:
   * -- if pending list is empty and no untaken nodes exist
   *      * Succeeded:  if there are succeeded nodes
   *      * Failed:     otherwise
   * -- Succeeded if there are at least K succeeded nodes which are `closer` to `target` then any of untaken ones
   * -- Continue in any other case
   *
   * Note that above means that [[org.abovobo.dht.Finder]] starts with [[org.abovobo.dht.Finder.State.Failed]]
   * state. It is responsibility of the owner of this object to handle this case.
   * 
   * XXX: FIXME: Update this logic, so it would wait for at least a round of pending requests, without this, we might often stop after first K responded nodes (on second round with alpha 3 and K 8, actually),
   *    thou there might be closer responses from pending requests.
   *    We can add "Pending" state which will not generate new requests nor finish recursion, until closer nodes are found or pending set is empty (or at least have less entries then current round 'width')
   * 
   */
  def state =
    if (this.pending.isEmpty && this.untaken.isEmpty) {
      if (this.succeeded.isEmpty) 
        Finder.State.Failed
      else 
        Finder.State.Succeeded
    } else if (this.succeeded.size >= K 
                && (this.untaken.isEmpty || this.ordering.lteq(this.succeeded.take(this.K).last, this.untaken.head))) { 
                // XXX: a) fix the case when we kill requests with pending closer nodes, b) fix case for GetPeers, when our goal is to get peers not nodes
      Finder.State.Succeeded
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
    val more = this.untaken.take(n)
    this.untaken --= more
    this.pending ++= more
    more
  }

  /** Returns maxium K succeeded nodes */
  def nodes = this.succeeded.take(this.K)

  /**
   * Returns token ([[scala.Option]]) for given node id.
   * @param id Node id to return token for.
   * @return   token for given node id.
   */
  def token(id: Integer160) = this._tokens.get(id)

  /** Returns map of tokens */
  def tokens: scala.collection.Map[Integer160, Token] = this._tokens

  /** Returns collection of peers */
  def peers: scala.collection.Traversable[Peer] = this._peers
  
  def iterate(): Unit
  
  private def add(nodes: Traversable[Node]) {
    // add all unseen nodes in both `seen` and `untaken` collections
    nodes foreach { node =>
      if (!this.seen.contains(node)) {
        this.seen += node
        this.untaken += node
      }
    }    
  }
}

/** Accompanying object */
object Finder {

  /** Defines enumeration of possible [[org.abovobo.dht.Finder]] states */
  object State extends Enumeration {
    val Continue, Succeeded, Failed = Value
  }

}

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

/**
 * This class collects data during recursive `find_node` or `get_peers` operations.
 *
 * @param target  A target 160-bit integer against which the find procedure is being ran.
 * @param K       A size of K-bucket used to calculate current state of finder.
 */
class Finder(val target: Integer160, K: Int, seeds: Traversable[InetSocketAddress]) {

  /// Dump all seeds into collection of untaken nodes with zero id
  this.seeds.foreach(this.untaken += new Node(Integer160.zero, _))

  /** Defines implicit [[scala.math.Ordering]] for [[org.abovobo.dht.Node]] instances */
  implicit val ordering = new scala.math.Ordering[Node] {
    override def compare(x: Node, y: Node): Int = {
      val x1 = Finder.this.target ^ x.id
      val y1 = Finder.this.target ^ y.id
      if (x1 < y1) -1 else if (x1 > y1) 1 else 0
    }
  }

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

    // store reporter into collection of succeded nodes
    this.succeeded += reporter

    // store node->token association
    if (!token.isEmpty) this.tokens += reporter.id -> token

    // add reported peers to internal collection
    this.peers ++= peers

    // add all unseen nodes in both `seen` and `untaken` collections
    nodes foreach { node =>
      if (!this.seen.contains(node)) {
        this.seen += node
        this.untaken += node
      }
    }
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
   */
  def state =
    if (this.pending.isEmpty && this.untaken.isEmpty) {
      if (this.succeeded.isEmpty) Finder.State.Failed
      else Finder.State.Succeeded
    } else if (this.ordering.lteq(this.succeeded.take(this.K).last, this.untaken.head)) {
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

  /// Collection of all nodes which were seen by means of node information sent with responses
  private val seen = new mutable.TreeSet[Node]

  /// Collection of nodes which has been taken but not reported yet
  private val pending = new mutable.TreeSet[Node]

  /// Collection of nodes which were seen but not yet taken to be queried
  private val untaken = new mutable.TreeSet[Node]

  /// Collection of nodes which reported successfully
  private val succeeded = new mutable.TreeSet[Node]

  /// Collection of node id -> token associations
  private val tokens = new mutable.HashMap[Integer160, Token]

  /// Collection of peers reported by queried nodes
  private val peers = new mutable.HashSet[Peer]
}

/** Accompanying object */
object Finder {

  /** Defines enumeration of possible [[org.abovobo.dht.Finder]] states */
  object State extends Enumeration {
    val Continue, Succeeded, Failed = Value
  }

}

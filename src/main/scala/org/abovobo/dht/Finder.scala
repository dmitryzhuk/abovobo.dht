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

import java.net.InetSocketAddress
import org.abovobo.integer.Integer160
import scala.collection.mutable

/**
 * Created by dmitryzhuk on 26.02.14.
 */
class Finder(K: Int, target: Integer160) {

  implicit val ordering = new scala.math.Ordering[Node] {
    override def compare(x: Node, y: Node): Int = {
      val x1 = Finder.this.target ^ x.id
      val y1 = Finder.this.target ^ y.id
      if (x1 < y1) -1 else if (x1 > y1) 1 else 0
    }
  }

  def report(reporter: Node, nodes: Traversable[Node], peers: Traversable[Peer], token: Token) = {

    // remove reporter from the collection of pending nodes
    // note that initially reporter may not be here at all
    this.pending -= reporter

    // store reporter into collection of succeded nodes
    this.succeeded += reporter

    // store node->token association
    this.tokens += reporter.id -> token

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

  private val pending = new mutable.TreeSet[Node]
  private val seen = new mutable.TreeSet[Node]
  private val untaken = new mutable.TreeSet[Node]
  private val succeeded = new mutable.TreeSet[Node]
  private val tokens = new mutable.HashMap[Integer160, Token]
  private val peers = new mutable.HashSet[Peer]
}

object Finder {
  object State extends Enumeration {
    val Continue, Succeeded, Failed = Value
  }
}

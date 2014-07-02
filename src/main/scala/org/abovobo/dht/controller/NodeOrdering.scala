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

import java.util.Comparator
import java.util.function.{Function, ToLongFunction, ToIntFunction, ToDoubleFunction}

import org.abovobo.dht.Node
import org.abovobo.integer.Integer160

/**
 * Provides utility for ordering nodes by their "distance" from given origin.
 * In Kademlia, the distance metric is XOR and the result is interpreted as an unsigned integer.
 * ``distance(A,B) = |A xor B|`` Smaller values are closer. So, this class measures the
 * distance of node IDs to given origin accordingly to this definition.
 *
 * @param origin
 */
class NodeOrdering(val origin: Integer160) extends math.Ordering[Node] {

  /** @inheritdoc */
  override def compare(x: Node, y: Node): Int = {
    val x1 = this.origin ^ x.id
    val y1 = this.origin ^ y.id
    if (x1 < y1) -1 else if (x1 > y1) 1 else 0
  }

  /// Overrides below are necessary to avoid IntelliJ IDEA to display error here
  /// due to lack of support of Java 8 @FunctionalInterface feature.
  override def reversed(): Comparator[Node] = super.reversed()
  override def thenComparingDouble(keyExtractor: ToDoubleFunction[_ >: Node]): Comparator[Node] =
    super.thenComparingDouble(keyExtractor)
  override def thenComparingInt(keyExtractor: ToIntFunction[_ >: Node]): Comparator[Node] =
    super.thenComparingInt(keyExtractor)
  override def thenComparingLong(keyExtractor: ToLongFunction[_ >: Node]): Comparator[Node] =
    super.thenComparingLong(keyExtractor)
  override def thenComparing(other: Comparator[_ >: Node]): Comparator[Node] = super.thenComparing(other)
  override def thenComparing[U](keyExtractor: Function[_ >: Node, _ <: U], keyComparator: Comparator[_ >: U]): Comparator[Node] =
    super.thenComparing(keyExtractor)
  override def thenComparing[U <: Comparable[_ >: U]](keyExtractor: Function[_ >: Node, _ <: U]): Comparator[Node] =
    super.thenComparing(keyExtractor)
}

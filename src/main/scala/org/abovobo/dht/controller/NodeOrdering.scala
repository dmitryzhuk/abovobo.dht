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

import org.abovobo.dht.NodeInfo
import org.abovobo.integer.Integer160

/**
 * Provides utility for ordering nodes by their "distance" from given origin.
 * In Kademlia, the distance metric is XOR and the result is interpreted as an unsigned integer.
 * ``distance(A,B) = |A xor B|`` Smaller values are closer. So, this class measures the
 * distance of node IDs to given origin accordingly to this definition.
 *
 * @param origin
 */
class NodeOrdering(val origin: Integer160) extends math.Ordering[NodeInfo] {

  /** @inheritdoc */
  override def compare(x: NodeInfo, y: NodeInfo): Int = {
    val x1 = this.origin ^ x.id
    val y1 = this.origin ^ y.id
    if (x1 < y1) -1 else if (x1 > y1) 1 else 0
  }

  /// Overrides below are necessary to avoid IntelliJ IDEA to display error here
  /// due to lack of support of Java 8 @FunctionalInterface feature.
  /*
  override def reversed(): Comparator[NodeInfo] = super.reversed()
  override def thenComparingDouble(keyExtractor: ToDoubleFunction[_ >: NodeInfo]): Comparator[NodeInfo] =
    super.thenComparingDouble(keyExtractor)
  override def thenComparingInt(keyExtractor: ToIntFunction[_ >: NodeInfo]): Comparator[NodeInfo] =
    super.thenComparingInt(keyExtractor)
  override def thenComparingLong(keyExtractor: ToLongFunction[_ >: NodeInfo]): Comparator[NodeInfo] =
    super.thenComparingLong(keyExtractor)
  override def thenComparing(other: Comparator[_ >: NodeInfo]): Comparator[NodeInfo] = super.thenComparing(other)
  override def thenComparing[U](keyExtractor: Function[_ >: NodeInfo, _ <: U], keyComparator: Comparator[_ >: U]): Comparator[NodeInfo] =
    super.thenComparing(keyExtractor)
  override def thenComparing[U <: Comparable[_ >: U]](keyExtractor: Function[_ >: NodeInfo, _ <: U]): Comparator[NodeInfo] =
    super.thenComparing(keyExtractor)
    */
}

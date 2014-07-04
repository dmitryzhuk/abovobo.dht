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

import org.abovobo.dht.Node
import scala.collection.mutable

/**
 * Defines collection of pending request rounds.
 *
 * @param rounds A backend mutable collection of rounds.
 */
class Rounds(val rounds: mutable.Queue[Round]) {

  /** Default constructor */
  def this() = this(new mutable.Queue[Round]())

  /**
   * Constructor which copies all initial rounds from given [[scala.collection.Traversable]].
   *
   * @param rounds Collection of initial rounds.
   */
  def this(rounds: scala.collection.Traversable[Round]) = this(new mutable.Queue[Round]() ++ rounds)

  /**
   * Puts new request round into a queue.
   *
   * @param round An instance of [[Round]] to enqueue.
   */
  def enqueue(round: Round) = this.rounds.enqueue(round)

  /**
   * Removes head element from queue and returns it.
   *
   * @return Removed head element of queue.
   */
  def dequeue() = this.rounds.dequeue()

  /** Returns head of the queue */
  def front = this.rounds.front

  /** Returns tail of the queue */
  def last = this.rounds.last

  /** Returns `true` is queue is empty */
  def isEmpty = this.rounds.isEmpty

  /** Returns `true` if queue is not empty */
  def nonEmpty = this.rounds.nonEmpty

  /**
   * Looks for the request sent to a given node and returns both [[Request]] and
   * corresponding [[Round]] instances if found, [[None]] otherwise.
   *
   * @param node An [[Node]] instance to find request to.
   *
   * @return Pair of [[Round]] and [[Request]] instances or [[None]] if not found.
   */
  def get(node: Node): Option[(Round, Request)] = this.rounds.flatMap(round => {
    round.get(node) match {
      case Some(request) => Some((round, request))
      case _ => None
    }
  }).headOption

}

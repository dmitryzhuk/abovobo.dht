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

import akka.actor.ActorSystem
import org.abovobo.dht.persistence._
import org.abovobo.jdbc.Closer._

/**
 * This class represents general wrapper over major components of DHT node:
 * Table, Agent Responder and Requester.
 *
 * @param as        An instance of [[ActorSystem]].
 * @param endpoint  An endpoint to listen for network messages at.
 * @param routers   Collection of initial routers used to start building DHT.
 * @param id        Optional parameter allowing to specify some unique identifier for this node.
 * @param sf        Storage factory function.
 */
class Node(val as: ActorSystem,
           val endpoint: InetSocketAddress,
           val routers: Traversable[InetSocketAddress],
           private val id: Long = 0L,
           sf: () => Storage) extends AutoCloseable {

  /// Collection of storage instances: 3 of them
  val storage = Array.fill[Storage](3) { this.sf() }

  /// Reference to Table actor
  val table = this.as.actorOf(Table.props(this.storage(0)), "table" + this.id)

  /// Reference to Requester actor
  val requester = this.as.actorOf(Requester.props(this.routers, this.storage(1), this.table), "requester" + this.id)

  /// Reference to Responder actor
  val responder = this.as.actorOf(Responder.props(this.storage(2), this.table), "responder" + this.id)

  /// Reference to Agent actor
  val agent = this.as.actorOf(Agent.props(this.endpoint, this.requester, this.responder), "agent" + this.id)

  /** @inheritdoc */
  override def close() = this.storage foreach { _.dispose() }
}

object NodeApp extends App {

  val system = ActorSystem("ABOVOBO-DHT-BASIC")
}

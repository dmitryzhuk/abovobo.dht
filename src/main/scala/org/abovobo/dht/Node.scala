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

import scala.concurrent.duration._

import akka.actor.{ActorSystem, Actor}
import org.abovobo.dht.persistence._

/**
 * This class represents general wrapper over major components of DHT node:
 * Table, Agent Responder and Requester.
 *
 * @param ds        An instance of [[DataSource]] class to be used by enclosed actors.
 * @param endpoint  An endpoint to listen for network messages at.
 * @param routers   Collection of initial routers used to start building DHT.
 * @param id        Optional parameter allowing to specify some unique identifier for this node.
 */
class Node(val as: ActorSystem,
           val ds: DataSource,
           val endpoint: InetSocketAddress,
           val routers: Traversable[InetSocketAddress],
           private val id: Long = 0L) {

  /// Collection of storage instances
  val storage = new h2.DynamicallyConnectedStorage(this.ds)

  /// Reference to Table actor
  val table = this.as.actorOf(Table.props(this.storage), "table" + this.id)

  /// Reference to Requester actor
  val requester = this.as.actorOf(Requester.props(this.routers, this.storage, this.table), "finder" + this.id)

  /// Reference to Responder actor
  val responder = this.as.actorOf(Responder.props(this.storage, this.table), "responder" + this.id)

  /// Reference to Agent actor
  val agent = this.as.actorOf(Agent.props(this.endpoint, this.requester, this.responder), "agent" + this.id)
}

object NodeApp extends App {

  val system = ActorSystem("AbovoboDhtNode")
}

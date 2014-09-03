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

import java.net.{InetAddress, InetSocketAddress}

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, Config}
import org.abovobo.dht.persistence._
import org.abovobo.jdbc.Closer._
import scala.concurrent.duration._

/**
 * This class represents general wrapper over major components of DHT node:
 * Table, Agent Responder and Requester.
 *
 * @param as        An instance of [[ActorSystem]].
 * @param routers   Collection of initial routers used to start building DHT.
 * @param sf        Storage factory function.
 * @param id        Optional parameter allowing to specify some unique identifier for this node.
 * @param overrides Optional instance (application-supplied) of [[Config]].
 */
class Node(val as: ActorSystem,
           val routers: Traversable[InetSocketAddress],
           sf: => Storage,
           id: Long = 0L,
           overrides: Config = null) extends AutoCloseable {

  /// Set up configuration
  val config = if (this.overrides == null) ConfigFactory.load() else ConfigFactory.load(this.overrides)

  /// Create endpoint address
  val endpoint =
    if (this.config.hasPath("dht.node.agent.address"))
      new InetSocketAddress(
        InetAddress.getByName(this.config.getString("dht.node.agent.address")),
        this.config.getInt("dht.node.agent.port"))
    else
      new InetSocketAddress(this.config.getInt("dht.node.agent.port"))

  /// Collection of storage instances: 3 of them
  val storage = Array.fill[Storage](3) { this.sf }

  /// Reference to Table actor
  val table = this.as.actorOf(

    Table.props(
      K         = this.config.getInt("dht.node.K"),
      timeout   = this.config.getInt("dht.node.table.timeout").milliseconds,
      delay     = this.config.getInt("dht.node.table.delay").milliseconds,
      threshold = this.config.getInt("dht.node.table.threshold"),
      storage   = this.storage(0)),

    "table" + this.id)

  /// Reference to Requester actor
  val requester = this.as.actorOf(

    Requester.props(
      K         = this.config.getInt("dht.node.K"),
      alpha     = this.config.getInt("dht.node.requester.alpha"),
      routers   = this.routers,
      reader    = this.storage(1),
      table     = this.table),

    "requester" + this.id)

  /// Reference to Responder actor
  val responder = this.as.actorOf(

    Responder.props(
      K         = this.config.getInt("dht.node.K"),
      period    = this.config.getInt("dht.node.responder.period").milliseconds,
      lifetime  = this.config.getInt("dht.node.responder.lifetime").milliseconds,
      storage   = this.storage(2),
      table     = this.table),

    "responder" + this.id)

  /// Reference to Agent actor
  val agent = this.as.actorOf(

    Agent.props(
      endpoint  = this.endpoint,
      timeout   = this.config.getInt("dht.node.agent.timeout").milliseconds,
      retry     = this.config.getInt("dht.node.agent.retry").milliseconds,
      requester = this.requester,
      responder = this.responder),

    "agent" + this.id)

  /** @inheritdoc */
  override def close() = this.storage foreach { _.dispose() }
}

object NodeApp extends App {

  val system = ActorSystem("ABOVOBO-DHT-BASIC")
}

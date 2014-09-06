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

import akka.actor.{Props, ActorSystem}
import akka.actor.ActorDSL._
import com.typesafe.config.ConfigFactory
import org.abovobo.dht.persistence.Storage
import org.abovobo.dht.persistence.h2.{DataSource, DynamicallyConnectedStorage}

import scala.collection.JavaConversions._
import scala.concurrent.Future

/**
 * This object creates multiple nodes which are locally interconnected.
 * The main purpose is to demonstrate how node binding, storing and finding values
 * would work in real networks.
 */
object NetworkEmulator extends App {

  // Load application configuration
  val config = ConfigFactory.load("network-emulator.conf").withFallback(ConfigFactory.load())

  // Initialize ActorSystem
  implicit val as = ActorSystem("NetworkEmulator", ConfigFactory.parseString("akka.loglevel=debug"))
  import this.as.dispatcher

  // Initialize DataSource
  val ds = DataSource(this.config.getString("dht.emulator.storage"))

  /**
   * Creates and new instance of Storage and tries to initialize it.
   *
   * @param id An id of the storage
   * @return instance of Storage
   */
  def storage(id: Long): Storage = {
    // initialize storage structure
    val s = new DynamicallyConnectedStorage(this.ds)

    // hide structure creation in try/catch block to avoid failure when
    // connecting to existing structure
    try {
      import org.abovobo.jdbc.Closer._

      // set working schema for the storage;
      // note that setting this before ensuring that schema exists
      // will only work as expected with DynamicallyConnectedStorage
      s.setSchema(this.config.getString("dht.emulator.schema") + id)

      // ensure that schema exists
      using(this.ds.connection) { c =>
        using(c.createStatement()) { s =>
          s.execute("create schema " + this.config.getString("dht.emulator.schema") + id)
        }
      }

      // create common tables
      s.execute("/tables-common.sql")

      // create tables specific to selected protocol which will be used in test
      if (this.config.getString("dht.emulator.ip") == "v4") {
        s.execute("/tables-ipv4.sql")
      } else if (this.config.getString("dht.emulator.ip") == "v6") {
        s.execute("/tables-ipv6.sql")
      } else {
        throw new IllegalArgumentException("Invalid IP version: " + this.config.getString("dht.emulator.ip"))
      }

      this.as.log.info("Initialized new storage with id=" + id)
    } catch {
      case t: Throwable => this.as.log.info("Initialized existing storage with id=" + id)
    }

    // function value is the storage itself
    s
  }

  // Optionally defined own router instance
  val router =
    if (this.config.hasPath("dht.emulator.router"))
      Some(new Node(
        this.as,
        Nil,
        () => this.storage(0),
        0,
        ConfigFactory.parseString(
          "dht.node.agent.port:" +
            this.config.getInt("dht.emulator.router.port") +
            (if (this.config.hasPath("dht.emulator.router.address"))
              ", dht.node.agent.address:\"" + this.config.getString("dht.emulator.router.address") + "\""
            else
              "")
        )
      ))
    else
      None

  // Starting Node id
  var id = 0

  // Starting port to listen at
  var port = this.config.getInt("dht.node.agent.port")

  // Collection of router addresses (single entry)
  val routers = (this.config.getConfigList("dht.emulator.routers") map { c =>
    new InetSocketAddress(InetAddress.getByName(c.getString("address")), c.getInt("port"))
  }).toList

  // Collection of nodes
  val nodes = Array.fill(this.config.getInt("dht.emulator.number")) {
    this.id += 1
    new Node(
      this.as,
      this.routers,
      () => this.storage(this.id),
      this.id,
      ConfigFactory.parseString("dht.node.agent.port=" + (this.port + this.id).toString)
    )
  }

  /** Actor handling [[org.abovobo.dht.UI.Shutdown]] event */
  val stopper = actor(new Act { become { case UI.Shutdown => Future { NetworkEmulator.this.shutdown() } } })

  // UI Actor initialization
  val ui = this.as.actorOf(
    Props(classOf[UI],
      new InetSocketAddress(
        InetAddress.getByName(this.config.getString("dht.node.ui.address")),
        this.config.getInt("dht.node.ui.port")),
      (this.router ++ this.nodes).toIndexedSeq,
      this.stopper),
    "ui")

  /** Shuts down application */
  def shutdown() = {
    println("Shutting down")
    this.nodes foreach { _.close() }
    this.router foreach { _.close() }
    this.as.shutdown()
    this.as.awaitTermination()
  }

  // Set up shutdown hook
  sys.addShutdownHook { this.shutdown() }
}

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
import com.typesafe.config.ConfigFactory
import org.abovobo.dht.persistence.Storage
import org.abovobo.dht.persistence.h2.{DynamicallyConnectedStorage, DataSource}

/**
 * This object creates multiple nodes which are locally interconnected.
 * The main purpose is to demonstrate how node binding, storing and finding values
 * would work in real networks.
 */
object NetworkEmulator extends App {

  // Initialize ActorSystem
  val as = ActorSystem("NetworkEmulator", ConfigFactory.parseString("akka.loglevel=debug"))

  // Initialize DataSource
  val ds = DataSource("jdbc:h2:~/db/network-emulator")

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
      // set working schema for the storage
      s.setSchema("ipv4_" + id)

      using(this.ds.connection) { c =>
        using(c.createStatement()) { s =>
          s.execute("create schema ipv4_" + id)
        }
      }
      // create common tables
      s.execute("/tables-common.sql")
      // create tables specific to IPv4 protocol which will be used in test
      s.execute("/tables-ipv4.sql")
      // ---
      this.as.log.info("Initialized new storage with id=" + id)
    } catch {
      case t: Throwable =>
        this.as.log.info("Initialized existing storage with id=" + id)
    }

    // function value is the storage itself
    s
  }

  // Starting Node id
  var id = 0

  // Starting port to listen at
  var port = 30000

  // Collection of router addresses (single entry)
  val routers = List(new InetSocketAddress(InetAddress.getLoopbackAddress, this.port + this.id))

  // Initialize router node
  val router = new Node(this.as, this.routers.head, Nil, this.id, this.storage(this.id))

  // Collection of nodes
  val nodes = Array.fill(4000) {
    this.id += 1
    new Node(
      this.as,
      new InetSocketAddress(InetAddress.getLoopbackAddress, this.port + this.id),
      this.routers,
      this.id,
      this.storage(this.id))
  }

  // Set up shutdown hook
  sys.addShutdownHook {
    println("Shutting down")
    this.nodes foreach { _.close() }
    this.router.close()
    this.as.shutdown()
    this.as.awaitTermination()
  }
}

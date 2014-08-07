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

import java.io.InputStreamReader
import java.net.{InetAddress, InetSocketAddress}

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.abovobo.dht.persistence.h2.DataSource
import org.abovobo.jdbc.Closer._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.mutable.ListBuffer

/**
 * Tests creation of DHT using this implementation
 */
class DHTTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("DHTTest", ConfigFactory.parseString("akka.loglevel=debug")))

  val startingPort = this.system.settings.config.getInt("port")
  val startingIndex = this.system.settings.config.getInt("index")

  val routerEndpoint =
    if (this.system.settings.config.hasPath("router"))
      new InetSocketAddress(InetAddress.getLoopbackAddress, this.system.settings.config.getInt("router"))
    else
      new InetSocketAddress(InetAddress.getLoopbackAddress, this.startingPort)

  val routerDS =
    if (!this.system.settings.config.hasPath("router")) {
      using(new InputStreamReader(this.getClass.getResourceAsStream("/tables.sql"))) { reader =>
        DataSource("jdbc:h2:~/db/dht-router", reader).close()

      }
      DataSource("jdbc:h2:~/db/dht-router;SCHEMA=ipv4")
    } else
      null

  def makeDS(id: Long) = {
    using(new InputStreamReader(this.getClass.getResourceAsStream("/tables.sql"))) { reader =>
      DataSource("jdbc:h2:~/db/dht-node" + id, reader).close()

    }
    DataSource("jdbc:h2:~/db/dht-node" + id + ";SCHEMA=ipv4")
  }
  val nodes: ListBuffer[(ActorRef, DataSource)] = new ListBuffer()
  var index = this.startingIndex
  val base = this.routerEndpoint.getPort

  override def beforeAll() = {

  }

  override def afterAll() = {
    println("AFTER ALL")
    Thread.sleep(300000)
    TestKit.shutdownActorSystem(this.system)
  }

  if (!this.system.settings.config.hasPath("router")) {
    "Router node" when {
      "just created" must {
        "log FindNode failure" in {
          val router = system.actorOf(
            Props(classOf[Node], this.routerDS, this.routerEndpoint, List.empty[InetSocketAddress], 0L),
            "router")
        }
      }
    }
  }

  "Normal node" when {
    "just created" must {
      "start FIND_NODE using router" in {
        for (i <- 0 until 1000) {
          val ds = makeDS(index)
          val node = system.actorOf(
            Props(classOf[Node],
              ds,
              new InetSocketAddress(InetAddress.getLoopbackAddress, this.base + index),
              List(this.routerEndpoint),
              index.toLong),
            "node" + index)
          nodes += (node -> ds)
          index += 1
          Thread.sleep(1)
        }
      }
    }
  }

}



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
import com.typesafe.config.ConfigFactory
import org.abovobo.dht.persistence.h2.{DataSource, PermanentlyConnectedStorage}
import org.abovobo.jdbc.Closer._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
 * Simple test displaying startup sequence of router node (with empty router list)
 */
class RouterTest extends WordSpecLike with Matchers with BeforeAndAfterAll {

  val as = ActorSystem("RouterTest", ConfigFactory.parseString("akka.loglevel=debug"))
  val ds = DataSource("jdbc:h2:~/db/router-test")

  override def beforeAll() = {

    // drop everything in the beginning and create working schema
    using(this.ds.connection) { c =>
      using(c.createStatement()) { s =>
        s.execute("drop all objects")
        s.execute("create schema ipv4")
        c.commit()
      }
    }

    // initialize storage structure
    using(new PermanentlyConnectedStorage(this.ds.connection)) { s =>
      // set working schema for the storage
      s.setSchema("ipv4")
      // create common tables
      s.execute("/tables-common.sql")
      // create tables specific to IPv4 protocol which will be used in test
      s.execute("/tables-ipv4.sql")
    }

    println()
  }

  override def afterAll() {
    Thread.sleep(5000)
    this.as.shutdown()
    this.as.awaitTermination()
  }

  "Router" when {
    "created" must {
      "display proper log of startup sequence" in {
        val node = new Node(
          this.as,
          new InetSocketAddress(0),
          Nil,
          0,
          new PermanentlyConnectedStorage(this.ds.connection).setSchema("ipv4")
        )
      }
    }
  }
}

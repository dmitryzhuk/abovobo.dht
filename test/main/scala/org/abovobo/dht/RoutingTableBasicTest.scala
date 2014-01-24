package org.abovobo.dht

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.abovobo.dht.persistence.{Writer, Reader, Storage, H2Storage}
import org.abovobo.integer.Integer160
import java.net.InetSocketAddress

/**
 * Unit test for RoutingTable Actor
 */
class RoutingTableBasicTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("RoutingTableTest"))

  private val h2 = new H2Storage("jdbc:h2:~/db/dht;SCHEMA=ipv4")

  val storage: Storage = this.h2
  val reader: Reader = this.h2
  val writer: Writer = this.h2

  lazy val table = this.system.actorOf(RoutingTable.props(this.reader, this.writer), "table")
  lazy val node = new Node(Integer160.zero, new InetSocketAddress(0))

  override def beforeAll() = {
    this.storage.open()
  }

  override def afterAll() = {
    this.storage.close()
    TestKit.shutdownActorSystem(this.system)
  }

  "RoutingTable Actor" when {

    "message Set is received" must {
      "set provided ID and purge routing table storage" in {
        println("Setting zero ID")
        this.table ! RoutingTable.Set(Integer160.zero)
        Thread.sleep(1000)
        this.reader.id() should be('defined)
        this.reader.id().get should be(Integer160.zero)
        this.reader.buckets() should have size 0
        this.reader.nodes() should have size 0
        println("Done.")
      }
    }

    "message Reset is received" must {
      "generate random id and purge routing table storage" in {
        println("Resetting ID")
        this.table ! RoutingTable.Reset()
        Thread.sleep(1000)
        this.reader.id() should be('defined)
        this.reader.id().get should not be Integer160.zero
        this.reader.buckets() should have size 0
        this.reader.nodes() should have size 0
        println("Done.")
      }
    }

    "message Set is received again" must {
      "set provided ID and purge routing table storage" in {
        println("Setting ID = maxval - 1")
        this.table ! RoutingTable.Set(Integer160.maxval - 1)
        Thread.sleep(1000)
        this.reader.id() should be('defined)
        this.reader.id().get should be(Integer160.maxval - 1)
        this.reader.buckets() should have size 0
        this.reader.nodes() should have size 0
        println("Done.")
      }
    }

    "message Received is received on empty table" must {
      "do nothing if network message kind was Fail or Error" in {
        println("Sending node with Error kind")
        this.table ! RoutingTable.Received(this.node, Message.Kind.Error)
        Thread.sleep(1000)
        this.reader.buckets() should have size 0
        this.reader.nodes() should have size 0
        println("Done.")
        println("Sending node with Fail kind")
        this.table ! RoutingTable.Received(this.node, Message.Kind.Fail)
        Thread.sleep(1000)
        this.reader.buckets() should have size 0
        this.reader.nodes() should have size 0
        println("Done.")
      }
      "insert zero bucket and then insert received node" in {
        println("Sending node with Query kind")
        this.table ! RoutingTable.Received(this.node, Message.Kind.Query)
        Thread.sleep(1000)
        this.reader.buckets() should have size 1
        this.reader.buckets().head._1 should be(Integer160.zero)
        this.reader.nodes() should have size 1
        val node = this.reader.nodes().head
        node.bucket should be(Integer160.zero)
        node.queried should be('defined)
        node.replied should not be 'defined
        node.failcount should be(0)
        println("Done.")
      }
    }

    "message Received is received on table with existing nodes" must {
      "update node's replied property if message kind is Reply" in {
        println("Sending same node with Reply kind")
        this.table ! RoutingTable.Received(this.node, Message.Kind.Reply)
        Thread.sleep(1000)
        this.reader.buckets() should have size 1
        this.reader.buckets().head._1 should be(Integer160.zero)
        this.reader.nodes() should have size 1
        val node = this.reader.nodes().head
        node.bucket should be(Integer160.zero)
        node.queried should be('defined)
        node.replied should be('defined)
        node.failcount should be(0)
        println("Done.")
      }
      "update node's failcount property if message kind is Fail" in {
        println("Sending same node with Reply kind")
        this.table ! RoutingTable.Received(this.node, Message.Kind.Fail)
        Thread.sleep(1000)
        this.reader.buckets() should have size 1
        this.reader.buckets().head._1 should be(Integer160.zero)
        this.reader.nodes() should have size 1
        val node = this.reader.nodes().head
        node.bucket should be(Integer160.zero)
        node.queried should be('defined)
        node.replied should be('defined)
        node.failcount should be(1)
        println("Done.")
      }
    }

    "multiple messages received" must {
      "split zero bucket and finally reject extra message" in {
        val start = Integer160.zero + 1
        for (i <- 0 to 6) {
          val node = new Node(start + i, new InetSocketAddress(0))
          table ! RoutingTable.Received(node, Message.Kind.Query)
        }
        val node = new Node(start + 7, new InetSocketAddress(0))
        table ! RoutingTable.Received(node, Message.Kind.Query)
        Thread.sleep(1000)
        this.reader.buckets() should have size 2
        this.reader.nodes() should have size 8
        this.reader.bucket(Integer160.zero) should have size 8
      }
    }
  }

}

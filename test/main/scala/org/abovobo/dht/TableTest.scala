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

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.abovobo.dht.persistence.{Writer, Reader, Storage, H2Storage}
import org.abovobo.integer.Integer160
import java.net.InetSocketAddress
import org.abovobo.dht.persistence.H2DataSource

/**
 * Unit test for RoutingTable Actor
 */
class TableTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("RoutingTableTest"))

  private val dataSource = H2DataSource.open("~/db/dht", true)
  private val h2 = new H2Storage(dataSource.getConnection)

  val storage: Storage = this.h2
  val reader: Reader = this.h2
  val writer: Writer = this.h2

  lazy val table = this.system.actorOf(Table.props(this.reader, this.writer, null), "table")
  lazy val node = new Node(Integer160.zero, new InetSocketAddress(0))

  override def beforeAll() = {
  }

  override def afterAll() = {
    this.storage.close()
    TestKit.shutdownActorSystem(this.system)
  }

  "RoutingTable Actor" when {

    "message Set is received" must {
      "set provided ID and purge routing table storage" in {
        this.table ! Table.Set(Integer160.zero)
        expectMsg(Table.Id(Integer160.zero))
        this.reader.id() should be('defined)
        this.reader.id().get should be(Integer160.zero)
        this.reader.buckets() should have size 0
        this.reader.nodes() should have size 0
      }
    }

    "message Reset is received" must {
      "generate random id and purge routing table storage" in {
        this.table ! Table.Reset()
        expectMsgClass(classOf[Table.Id])
        this.reader.id() should be('defined)
        this.reader.id().get should not be Integer160.zero
        this.reader.buckets() should have size 0
        this.reader.nodes() should have size 0
      }
    }

    "message Set is received again" must {
      "set provided ID and purge routing table storage" in {
        this.table ! Table.Set(Integer160.maxval - 1)
        expectMsg(Table.Id(Integer160.maxval - 1))
        this.reader.id() should be('defined)
        this.reader.id().get should be(Integer160.maxval - 1)
        this.reader.buckets() should have size 0
        this.reader.nodes() should have size 0
      }
    }

    "message Received is received on empty table" must {
      "do nothing if network message kind was Error" in {
        this.table ! Table.Received(this.node, Message.Kind.Error)
        expectMsg(Table.Rejected)
        this.reader.buckets() should have size 0
        this.reader.nodes() should have size 0
      }
      "do nothing if network message kind was Fail" in {
        this.table ! Table.Received(this.node, Message.Kind.Fail)
        expectMsg(Table.Rejected)
        this.reader.buckets() should have size 0
        this.reader.nodes() should have size 0
      }
      "insert zero bucket and then insert received node" in {
        this.table ! Table.Received(this.node, Message.Kind.Query)
        expectMsg(Table.Inserted(Integer160.zero))
        this.reader.buckets() should have size 1
        this.reader.buckets().head._1 should be(Integer160.zero)
        this.reader.nodes() should have size 1
        val node = this.reader.nodes().head
        node.bucket should be(Integer160.zero)
        node.queried should be('defined)
        node.replied should not be 'defined
        node.failcount should be(0)
      }
    }

    "message Received is received on table with existing nodes" must {
      "update node's replied property if message kind is Response" in {
        this.table ! Table.Received(this.node, Message.Kind.Response)
        expectMsg(Table.Updated)
        this.reader.buckets() should have size 1
        this.reader.buckets().head._1 should be(Integer160.zero)
        this.reader.nodes() should have size 1
        val node = this.reader.nodes().head
        node.bucket should be(Integer160.zero)
        node.queried should be('defined)
        node.replied should be('defined)
        node.failcount should be(0)
      }
      "update node's failcount property if message kind is Fail" in {
        this.table ! Table.Received(this.node, Message.Kind.Fail)
        expectMsg(Table.Updated)
        this.reader.buckets() should have size 1
        this.reader.buckets().head._1 should be(Integer160.zero)
        this.reader.nodes() should have size 1
        val node = this.reader.nodes().head
        node.bucket should be(Integer160.zero)
        node.queried should be('defined)
        node.replied should be('defined)
        node.failcount should be(1)
      }
    }

    "multiple messages received" must {
      "split zero bucket and finally reject extra message" in {
        val start = Integer160.zero + 1
        for (i <- 0 to 6) {
          val node = new Node(start + i, new InetSocketAddress(0))
          table ! Table.Received(node, Message.Kind.Query)
          expectMsg(Table.Inserted(Integer160.zero))
        }
        val node = new Node(start + 7, new InetSocketAddress(0))
        table ! Table.Received(node, Message.Kind.Query)
        expectMsg(Table.Split(Integer160.zero, Integer160.maxval >> 1))
        expectMsg(Table.Rejected)
        this.reader.buckets() should have size 2
        this.reader.nodes() should have size 8
        this.reader.bucket(Integer160.zero) should have size 8
      }
    }
  }

}

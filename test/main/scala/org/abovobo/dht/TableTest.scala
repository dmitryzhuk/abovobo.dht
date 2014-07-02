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
import org.abovobo.dht.controller.Controller
import org.abovobo.dht.message.Message
import org.abovobo.dht.persistence.h2.{Storage, DataSource}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.abovobo.integer.Integer160
import java.net.InetSocketAddress
import akka.actor.Inbox
import scala.concurrent.duration._

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

  private val ds = DataSource("jdbc:h2:~/db/dht;SCHEMA=ipv4")
  private val h2 = new Storage(ds.connection)
  private val reader = h2

  val controllerInbox = Inbox.create(system)

  lazy val table = this.system.actorOf(Table.props(
      K = 8,
      timeout = 60.seconds,
      delay = 30.seconds,
      threshold = 3,
      reader = this.h2,
      writer = this.h2,
      controller = controllerInbox.getRef()),
    "table")
  lazy val node = new Node(Integer160.zero, new InetSocketAddress(0))

  override def beforeAll() = {
    // Do nothing
  }

  override def afterAll() = {
    h2.close()
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

    "table is being filled with nodes" must {
      "have like a whole lot of them in the end :)" in {

        // Delete previously added node from the table
        this.h2.delete(this.node.id)
        this.h2.commit()

        // start with 1 as a node id and 0 as an initial bucket
        var start = Integer160.zero + 1
        var bucket = Integer160.zero
        var repeat = true

        while (repeat) {
          // insert 8 nodes assuming they will be put into a current bucket
          for (i <- 0 to 7) {
            val node = new Node(start + i, new InetSocketAddress(0))
            table ! Table.Received(node, Message.Kind.Query)
            expectMsg(Table.Inserted(bucket))
          }

          // try to insert 9th node which must cause
          // -- pair of messages Split, Reject if there is a more space for nodes in the table
          // -- single Reject message if there is no room to split buckets further
          val node = new Node(start + 8, new InetSocketAddress(0))
          table ! Table.Received(node, Message.Kind.Query)
          expectMsgType[Table.Result] match {
            case Table.Split(was, now) =>
              bucket = now
              start = bucket
              expectMsg(Table.Rejected)
            case Table.Rejected =>
              repeat = false
            case _ =>
              this.fail("Unexpected table behaviour")
          }
        }

        this.reader.buckets() should have size 157
        this.reader.nodes() should have size 8 * 157
      }
    }

    "after timeout expires table" must {
      "send Refresh events and cause FindNode received by Controller" in {
        this.controllerInbox.receive(60.seconds) match {
          case Controller.FindNode(target) =>
            for (i <- 0 until 160) this.controllerInbox.receive(1.second)
          case _ => this.fail("Unexpected message to controller")
        }
      }
    }
  }

}

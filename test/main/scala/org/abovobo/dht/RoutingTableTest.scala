package org.abovobo.dht

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.abovobo.integer.Integer160

/**
 * Tests RoutingTable Actor
 */
class RoutingTableTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with MustMatchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("RoutingTableTest"))

  override def afterAll() = {
    TestKit.shutdownActorSystem(this.system)
  }

  "A RoutingTable Actor" must {
    val table = this.system.actorOf(RoutingTable.props())
    val node = new Node(Integer160.zero, Some(new Endpoint(new Array[Byte](6))), None, None, None)

    "report Set Done" in {
      val id = Integer160.maxval - 1
      table ! RoutingTable.SetId(id)
      expectMsg(RoutingTable.Done(id))
    }
    "report Inserted" in {
      table ! RoutingTable.GotQuery(node)
      expectMsg(RoutingTable.Report(node, RoutingTable.Result.Inserted))
    }
    "report Updated" in {
      table ! RoutingTable.GotReply(node)
      expectMsg(RoutingTable.Report(node, RoutingTable.Result.Updated))
    }
    "report Updated when GotFail" in {
      table ! RoutingTable.GotFail(node)
      expectMsg(RoutingTable.Report(node, RoutingTable.Result.Updated))
    }
    "report Deferred" in {
      val start = Integer160.zero + 1
      for (i <- 0 to 6) {
        val node = new Node(start + i, Some(new Endpoint(new Array[Byte](6))), None, None, None)
        table ! RoutingTable.GotQuery(node)
        expectMsg(RoutingTable.Report(node, RoutingTable.Result.Inserted))
      }
      val node = new Node(start + 7, Some(new Endpoint(new Array[Byte](6))), None, None, None)
      table ! RoutingTable.GotQuery(node)
      expectMsg(RoutingTable.Report(node, RoutingTable.Result.Deferred))
    }
    "report Rejected" in {
      val start = Integer160.maxval
      for (i <- 0 to 7) {
        val node = new Node(start - i, Some(new Endpoint(new Array[Byte](6))), None, None, None)
        table ! RoutingTable.GotQuery(node)
        expectMsg(RoutingTable.Report(node, RoutingTable.Result.Inserted))
      }
      val node = new Node(Integer160.zero + 8, Some(new Endpoint(new Array[Byte](6))), None, None, None)
      table ! RoutingTable.GotQuery(node)
      expectMsg(RoutingTable.Report(node, RoutingTable.Result.Rejected))
    }
  }
}

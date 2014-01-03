package org.abovobo.dht

import akka.actor.{Props, ActorSystem}
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
    val node = new Node(Integer160.random, Some(new Endpoint(new Array[Byte](6))), None, None, None)

    "report Inserted" in {
      table ! RoutingTable.GotQuery(node)
      expectMsg(RoutingTable.Report(node, RoutingTable.Result.Inserted))
    }

    "report Updated" in {
      table ! RoutingTable.GotReply(node)
      expectMsg(RoutingTable.Report(node, RoutingTable.Result.Updated))
    }
  }
}

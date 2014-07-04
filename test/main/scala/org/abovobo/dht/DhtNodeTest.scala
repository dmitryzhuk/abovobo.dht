package org.abovobo.dht

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpecLike
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import java.net.InetSocketAddress
import java.net.InetAddress
import scala.concurrent.duration._
import org.abovobo.integer.Integer160


class DhtNodeTest(system: ActorSystem) extends TestKit(system) with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("DhtNodeTest", ConfigFactory.parseString("akka.loglevel=debug")))

  val node = system.actorOf(DhtNode.props(new InetSocketAddress(InetAddress.getLocalHost, 10000)))
  
  "DhtNode" when {
    "got Describe message" must {
      "reply with info" in {
        node ! DhtNode.Describe
        expectMsgPF (5 seconds, "NodeInfo") {
          case info: DhtNode.NodeInfo =>
            info.self.id should not equal Integer160.zero
          case x: Any => fail("Unexpected response " + x)
        }
            
      }
    }
    "got Stop message" must {
      "stop" in {
        node ! DhtNode.Stop
        Thread.sleep(3000)
        node ! DhtNode.Describe
        expectNoMsg(3 seconds)
      }
    }
  }
}
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

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import akka.io.{IO, Udp}
import java.net.{InetAddress, InetSocketAddress}
import scala.concurrent.duration._
import org.abovobo.integer.Integer160

class RemotePeer(val endpoint: InetSocketAddress) extends Actor with ActorLogging {

  import this.context.system

  override def preStart() = IO(Udp) ! Udp.Bind(self, this.endpoint)

  override def receive = {
    case Udp.Bound(l) =>
      this.log.info("Bound with local address {}", l)
      this.context.become(this.ready(this.sender))
  }

  def ready(socket: ActorRef): Actor.Receive = {
    case Udp.Send(data, r, ack) =>
      this.log.info("Sending to " + r + ": " + data.toString())
      socket ! Udp.Send(data, r, ack)
    case Udp.Received(data, r) =>
      this.log.info("Received from " + r + ": " + data.toString())
      this.context.actorSelection("../../system/testActor*") ! Udp.Received(data, r)
    case Udp.Unbind  =>
      this.log.info("Unbinding")
      socket ! Udp.Unbind
    case Udp.Unbound =>
      this.log.info("Unbound")
  }
}

class DummyController extends Actor with ActorLogging {

  override def receive = {
    case Controller.Received(message) =>
      this.log.info("Received message " + message)
      this.context.actorSelection("../../system/testActor*") ! Controller.Received(message)
    case Controller.Fail(query) =>
      this.log.info("Failed to receive response to " + query)
      this.context.actorSelection("../../system/testActor*") ! Controller.Fail(query)
  }

}

/**
 * Unit test for [[org.abovobo.dht.NetworkAgent]]
 */
class NetworkAgentTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("NetworkAgentTest"))

  val remote = new InetSocketAddress(InetAddress.getLoopbackAddress, 20000)
  val local = new InetSocketAddress(InetAddress.getLoopbackAddress, 20001)

  val na = this.system.actorOf(NetworkAgent.props(local, 10.seconds), "agent")
  val rp = this.system.actorOf(Props(classOf[RemotePeer], remote), "peer")
  val dc = this.system.actorOf(Props(classOf[DummyController]), "controller")

  override def beforeAll() = {
  }

  override def afterAll() = {
    this.rp ! Udp.Unbind
    this.na ! Udp.Unbind
    TestKit.shutdownActorSystem(this.system)
  }

  val factory = new TIDFactory

  "NetworkAgent Actor" when {

    "command Send(Ping) is issued" must {
      val query = new Query.Ping(factory.next(), Integer160.maxval)
      "serialize message and send it to remote peer" in {
        na ! NetworkAgent.Send(query, remote)
        expectMsgClass(classOf[Udp.Received])
      }
      "complete transaction and notify Controller after receiving network response" in {
        rp ! Udp.Send(NetworkAgent.serialize(new Response.Ping(query.tid, Integer160.zero)), local)
        expectMsgClass(classOf[Controller.Received])
      }
    }

    "command Send(FindNode) is issued" must {
      val query = new Query.FindNode(factory.next(), Integer160.maxval, target=Integer160.random)
      "serialize message and send it to remote peer" in {
        na ! NetworkAgent.Send(query, remote)
        expectMsgClass(classOf[Udp.Received])
      }
      "complete transaction and notify Controller after not receiving network response" in {
        expectMsgClass(12.seconds, classOf[Controller.Fail])
      }
    }

  }
}

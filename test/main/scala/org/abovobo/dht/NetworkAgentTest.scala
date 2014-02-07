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
import akka.io.{IO, Udp}
import akka.testkit.{ImplicitSender, TestKit}

import java.net.{InetAddress, InetSocketAddress}

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.abovobo.integer.Integer160

import scala.concurrent.duration._

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
    case Udp.Unbind =>
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

    "command Send(Query.Ping) is issued" must {
      val tid = factory.next()
      val query = new Query.Ping(tid, Integer160.maxval)
      val packet: Array[Byte] = "d1:ad2:id20:".getBytes("UTF-8") ++ Integer160.maxval.toArray ++
        "e1:q4:ping1:t2:".getBytes("UTF-8") ++ tid.toArray ++ "1:y1:qe".getBytes("UTF-8")

      "serialize message and send it to remote peer" in {
        na ! NetworkAgent.Send(query, remote)
        expectMsgPF() {
          case Udp.Received(data, address) =>
            packet should equal(data.toArray)
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }

      "complete transaction and notify Controller after receiving network response" in {
        rp ! Udp.Send(NetworkAgent.serialize(new Response.Ping(query.tid, Integer160.zero)), local)
        expectMsgPF() {
          case Controller.Received(message) =>
            message match {
              case ping: Response.Ping =>
                ping.id should be(Integer160.zero)
                ping.tid.toArray should equal(tid.toArray)
              case a: Any =>
                this.fail("Wrong message type " + a.getClass)
            }
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }

    }

    "command Send(Query.FindNode) is issued" must {
      val tid = factory.next()
      val target = Integer160.random
      val query = new Query.FindNode(tid, id = Integer160.maxval, target = target)
      val packet: Array[Byte] = "d1:ad2:id20:".getBytes("UTF-8") ++ Integer160.maxval.toArray ++
        "6:target20:".getBytes("UTF-8") ++ target.toArray ++
        "e1:q9:find_node1:t2:".getBytes("UTF-8") ++ tid.toArray ++ "1:y1:qe".getBytes("UTF-8")

      "serialize message and send it to remote peer" in {
        na ! NetworkAgent.Send(query, remote)
        expectMsgPF() {
          case Udp.Received(data, address) =>
            packet should equal(data.toArray)
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }

      "complete transaction and notify Controller after not receiving network response" in {
        expectMsgPF(12.seconds) {
          case Controller.Fail(q: Query) =>
            q should be theSameInstanceAs query
          case a: Any =>
            fail("Wrong message type " + a.getClass)
        }
      }
    }

  }
}

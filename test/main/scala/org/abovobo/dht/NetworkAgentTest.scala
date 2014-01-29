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
import akka.util.ByteString
import akka.io.Udp.SimpleSender

class Sender(val destination: InetSocketAddress) extends Actor with ActorLogging {

  import this.context.system

  IO(Udp) ! Udp.SimpleSender

  def receive = {
    case Udp.SimpleSenderReady =>
      this.log.info("Sender ready")
      this.context.become(ready(sender))
  }

  def ready(send: ActorRef): Receive = {
    case msg: String =>
      this.log.info("Sending " + msg)
      send ! Udp.Send(ByteString(msg), destination)
  }
}

class RemotePeer(val endpoint: InetSocketAddress) extends Actor with ActorLogging {

  import this.context.system

  IO(Udp) ! Udp.Bind(self, this.endpoint)

  override def receive = {
    case Udp.Bound(l) =>
      this.log.info("Bound with local address {}", l)
      this.context.become(this.ready(this.sender))
  }

  def ready(socket: ActorRef): Actor.Receive = {
    case Udp.Received(data, r) =>
      this.log.info("Received: " + data.toString())
      this.context.actorSelection("../../system/testActor*") ! Udp.Received(data, r)
    case Udp.Unbind  =>
      this.log.info("Unbinding")
      socket ! Udp.Unbind
    case Udp.Unbound =>
      this.log.info("Unbound")
      context.stop(self)
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
  //val ss = this.system.actorOf(Props(classOf[Sender], remote), "sender")
  val rp = this.system.actorOf(Props(classOf[RemotePeer], remote), "peer")

  override def beforeAll() = {
    //println(this.self.path)
  }

  override def afterAll() = {
    rp ! Udp.Unbind
    TestKit.shutdownActorSystem(this.system)
  }

  val factory = new TIDFactory

  "NetworkAgent Actor" when {

    "command Send is received" must {

      "serialize message and send it to remote peer" in {
        na ! NetworkAgent.Send(new Query.Ping(factory.next(), Integer160.maxval), remote)
        //ss ! "Test message"
        expectMsgClass(classOf[Udp.Received])
        //Thread.sleep(10000)
        //ss ! "Test"
        /*
        Thread.sleep(1000)
        println("Sending")
        implicit val system = this.system
        IO(Udp) ! Udp.Send(ByteString("Hello"), remote)
        expectMsg("ok")
        */
      }

    }

  }
}

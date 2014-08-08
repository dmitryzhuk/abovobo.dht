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

import java.net.{InetAddress, InetSocketAddress}

import akka.actor._
import akka.io.{Udp, IO}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import scala.concurrent.duration._

object UdpListener {
  sealed trait Command
  case object Fail extends Command
  case object Start extends Command

  sealed trait Event
  case object Bound extends Event
}


class UdpListener(val endpoint: InetSocketAddress) extends Actor with ActorLogging {

  import this.context.system
  import this.context.dispatcher
  import UdpListener._

  var delay: Option[Cancellable] = None
  var socket: Option[ActorRef] = None

  override def preStart() = {
    this.log.debug("PreStart")
    self ! Start
  }

  override def postStop() = {
    this.log.debug("PostStop")
  }

  override def preRestart(reason: Throwable, message: Option[Any]) = {
    this.log.debug("PreRestart {}", this.socket)
    this.socket.foreach(_ ! Udp.Unbind)
  }

  override def postRestart(reason: Throwable) = {
    this.log.debug("PostRestart")
    self ! Start
  }

  override def receive = {
    case Start =>
      this.log.debug("Binding")
      IO(Udp) ! Udp.Bind(self, this.endpoint)
      //this.delay = Some(system.scheduler.scheduleOnce(1.seconds, self, Fail))

    case Udp.CommandFailed(cmd) => cmd match {
      case Udp.Bind(h, l, o) =>
        this.log.debug("Command Udp.Bind has failed for {} at {}", h, l)
        this.delay.foreach(_.cancel())
        this.delay = Some(system.scheduler.scheduleOnce(1.second, self, Start))
    }

    case Udp.Bound(l) =>
      this.log.debug("Bound with local address {}", l)
      this.socket = Some(this.sender())
      this.delay.foreach(_.cancel())
      this.context.watch(this.sender())
      this.context.become(this.ready(this.sender()))
      this.context.actorSelection("../../../system/testActor*") ! Bound

    case Fail =>
      this.log.debug("Failing from root")
      throw new RuntimeException()
  }

  def ready(socket: ActorRef): Actor.Receive = {
    case Udp.Send(data, r, ack) =>
      this.log.debug("Sending to " + r + ": " + data.toString())
      socket ! Udp.Send(data, r, ack)
    case Udp.Received(data, r) =>
      this.log.debug("Received from " + r + ": " + data.toString())
      this.context.actorSelection("../../../system/testActor*") ! Udp.Received(data, r)
    case Udp.Unbind =>
      this.log.debug("Unbinding")
      socket ! Udp.Unbind
    case Udp.Unbound =>
      this.log.debug("Unbound")
    case Fail =>
      this.log.debug("Failing")
      throw new RuntimeException()
  }
}

/*
class Supervisor extends Actor with ActorLogging {
  import akka.actor.OneForOneStrategy
  import akka.actor.SupervisorStrategy._

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _ => Stop
    }

  var p: Props = null
  override def receive = {
    case p: Props =>
      this.p = p
      val child = context.actorOf(p, "peer")
      this.context.watch(child)
      sender() ! child
    case Terminated(ref) =>
      this.log.debug("Terminated " + ref)
      val child = context.actorOf(this.p, "peer")
      this.context.watch(child)
      this.context.actorSelection("../../../system/testActor*") ! child
  }

}
*/

/**
 * Testing UDP actor behaviour
 */
class UdpTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  import UdpListener._

  def this() = this(ActorSystem("UdpTest", ConfigFactory.parseString("akka.loglevel=debug")))

  val remote = new InetSocketAddress(InetAddress.getLoopbackAddress, 30000)

  override def afterAll() = {
    //this.peer ! Udp.Unbind
    //Thread.sleep(1000)
    TestKit.shutdownActorSystem(this.system)
  }

  "UdpListener actor" when {
    var peer = system.actorOf(Props(classOf[UdpListener], remote), "peer")
    "created" must {
      "send `Bound` message" in {

        expectMsg(15.seconds, Bound)
        /*
        peer ! Fail
        peer = expectMsgType[ActorRef](15.seconds)
        peer = expectMsgType[ActorRef](15.seconds)
        expectMsg(15.seconds, Bound)
        peer ! PoisonPill
        peer = expectMsgType[ActorRef](15.seconds)
        expectMsg(15.seconds, Bound)
        */
      }
    }
    "failed" must {
      "restart and the send `Bound` message" in {
        peer ! Fail
        expectMsg(15.seconds, Bound)
      }
    }
  }
}

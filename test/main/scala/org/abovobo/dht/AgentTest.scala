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

import org.abovobo.dht
import org.abovobo.dht.controller.Controller
import org.abovobo.dht.message.{Response, Query}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.abovobo.integer.Integer160

import scala.concurrent.duration._
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

class RemotePeer(val endpoint: InetSocketAddress) extends Actor with ActorLogging {

  import this.context.system

  override def preStart() = IO(Udp) ! Udp.Bind(self, this.endpoint)

  override def receive = {
    case Udp.Bound(l) =>
      this.log.debug("Bound with local address {}", l)
      this.context.become(this.ready(this.sender()))
  }

  def ready(socket: ActorRef): Actor.Receive = {
    case Udp.Send(data, r, ack) =>
      this.log.debug("Sending to " + r + ": " + data.toString())
      socket ! Udp.Send(data, r, ack)
    case Udp.Received(data, r) =>
      this.log.debug("Received from " + r + ": " + data.toString())
      this.context.actorSelection("../../system/testActor*") ! Udp.Received(data, r)
    case Udp.Unbind =>
      this.log.debug("Unbinding")
      socket ! Udp.Unbind
    case Udp.Unbound =>
      this.log.debug("Unbound")
  }
}

/**
 * Unit test for [[org.abovobo.dht.Agent]]
 */
class AgentTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("AgentTest"/*, ConfigFactory.parseString("akka.loglevel=debug")*/))

  val remote = new InetSocketAddress(InetAddress.getLoopbackAddress, 30000)
  val local = new InetSocketAddress(InetAddress.getLoopbackAddress, 30001)

  val controllerInbox = Inbox.create(system)

  val agent = this.system.actorOf(Agent.props(local, 10.seconds, controllerInbox.getRef()), "agent")
  val peer = this.system.actorOf(Props(classOf[RemotePeer], remote), "peer")

  override def beforeAll() = {
  }

  override def afterAll() = {
    this.peer ! Udp.Unbind
    this.agent ! Udp.Unbind
    Thread.sleep(1000)
    TestKit.shutdownActorSystem(this.system)
  }

  val factory = new TIDFactory

  "Agent Actor" when {

    "command Send(Query.Ping) is issued" must {
      val tid = factory.next()
      val query = new Query.Ping(tid, Integer160.maxval)
      val packet: Array[Byte] = "d1:ad2:id20:".getBytes("UTF-8") ++ Integer160.maxval.toArray ++
        "e1:q4:ping1:t2:".getBytes("UTF-8") ++ tid.toArray ++ "1:y1:qe".getBytes("UTF-8")

      "serialize message and send it to remote peer" in {
        agent ! Agent.Send(query, remote)
        expectMsgPF() {
          case Udp.Received(data, address) =>
            packet should equal(data.toArray)
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }

      "complete transaction and notify Controller after receiving network response" in {
        peer ! Udp.Send(Agent.serialize(new Response.Ping(query.tid, Integer160.zero)), local)
        controllerInbox.receive(1.second) match {
          case Controller.Received(message, address) =>
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
        agent ! Agent.Send(query, remote)
        expectMsgPF() {
          case Udp.Received(data, address) =>
            packet should equal(data.toArray)
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }

      "complete transaction and notify Controller after not receiving network response" in {
        controllerInbox.receive(15.seconds) match {
          case Controller.Failed(q: Query) =>
            q should be theSameInstanceAs query
          case a: Any =>
            fail("Wrong message type " + a.getClass)
        }
      }
    }

    "another command Send(Query.FindNode) is issued" must {
      val tid = factory.next()
      val target = Integer160.random
      val query = new Query.FindNode(tid, id = Integer160.maxval, target = target)
      val packet: Array[Byte] = "d1:ad2:id20:".getBytes("UTF-8") ++ Integer160.maxval.toArray ++
        "6:target20:".getBytes("UTF-8") ++ target.toArray ++
        "e1:q9:find_node1:t2:".getBytes("UTF-8") ++ tid.toArray ++ "1:y1:qe".getBytes("UTF-8")

      "serialize message and send it to remote peer" in {
        agent ! Agent.Send(query, remote)
        expectMsgPF() {
          case Udp.Received(data, address) =>
            packet should equal(data.toArray)
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }

      "complete transaction and notify Controller after receiving network response" in {
        peer ! Udp.Send(Agent.serialize(new Response.FindNode(query.tid, Integer160.zero,
          nodes = Array(new NodeInfo(Integer160.zero, new InetSocketAddress(0))))), local)
        controllerInbox.receive(10.seconds) match {
          case Controller.Received(message, address) =>
            message match {
              case fn: Response.FindNode =>
                fn.id should be(Integer160.zero)
                fn.tid.toArray should equal(tid.toArray)
                fn.nodes should have size 1
                fn.nodes(0).id should be(Integer160.zero)
              case a: Any =>
                this.fail("Wrong message type " + a.getClass)
            }
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }

    }

    "command Send(Query.GetPeers) is issued" must {
      val tid = factory.next()
      val infohash = Integer160.random
      val query = new Query.GetPeers(tid, id = Integer160.maxval, infohash = infohash)
      val packet: Array[Byte] = "d1:ad2:id20:".getBytes("UTF-8") ++ Integer160.maxval.toArray ++
        "9:info_hash20:".getBytes("UTF-8") ++ infohash.toArray ++
        "e1:q9:get_peers1:t2:".getBytes("UTF-8") ++ tid.toArray ++ "1:y1:qe".getBytes("UTF-8")

      "serialize message and send it to remote peer" in {
        agent ! Agent.Send(query, remote)
        expectMsgPF() {
          case Udp.Received(data, address) =>
            packet should equal(data.toArray)
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }

      "complete transaction and notify Controller after receiving network response" in {
        peer ! Udp.Send(Agent.serialize(new Response.GetPeersWithNodes(query.tid, Integer160.zero,
          token = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
          nodes = Array(new NodeInfo(Integer160.zero, new InetSocketAddress(0))))), local)
        controllerInbox.receive(10.seconds) match {
          case Controller.Received(message, address) =>
            message match {
              case gp: Response.GetPeersWithNodes =>
                gp.id should be(Integer160.zero)
                gp.tid.toArray should equal(tid.toArray)
                gp.nodes should have size 1
                gp.nodes(0).id should be(Integer160.zero)
                gp.token should equal(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
              case a: Any =>
                this.fail("Wrong message type " + a.getClass)
            }
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }

    }

    "another command Send(Query.GetPeers) is issued" must {
      val tid = factory.next()
      val infohash = Integer160.random
      val query = new Query.GetPeers(tid, id = Integer160.maxval, infohash = infohash)
      val packet: Array[Byte] = "d1:ad2:id20:".getBytes("UTF-8") ++ Integer160.maxval.toArray ++
        "9:info_hash20:".getBytes("UTF-8") ++ infohash.toArray ++
        "e1:q9:get_peers1:t2:".getBytes("UTF-8") ++ tid.toArray ++ "1:y1:qe".getBytes("UTF-8")

      "serialize message and send it to remote peer" in {
        agent ! Agent.Send(query, remote)
        expectMsgPF() {
          case Udp.Received(data, address) =>
            packet should equal(data.toArray)
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }

      "complete transaction and notify Controller after receiving network response" in {
        peer ! Udp.Send(Agent.serialize(new Response.GetPeersWithValues(query.tid, Integer160.zero,
          token = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
          values = Array(new InetSocketAddress(0)))), local)
        controllerInbox.receive(10.seconds) match {
          case Controller.Received(message, address) =>
            message match {
              case gp: Response.GetPeersWithValues =>
                gp.id should be(Integer160.zero)
                gp.tid.toArray should equal(tid.toArray)
                gp.values should have size 1
                gp.values(0) should be(new InetSocketAddress(0))
                gp.token should equal(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
              case a: Any =>
                this.fail("Wrong message type " + a.getClass)
            }
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }

    }

    "command Send(Query.AnnouncePeer) is issued" must {
      val tid = factory.next()
      val infohash = Integer160.random
      val token = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
      val query = new Query.AnnouncePeer(tid, id = Integer160.maxval,
        infohash = infohash, port = 1000,
        token = token,
        implied = true)
      val packet: Array[Byte] = "d1:ad2:id20:".getBytes("UTF-8") ++ Integer160.maxval.toArray ++
        "12:implied_porti1e".getBytes("UTF-8") ++
        "9:info_hash20:".getBytes("UTF-8") ++ infohash.toArray ++
        "4:porti1000e".getBytes("UTF-8") ++
        "5:token10:".getBytes("UTF-8") ++ token ++
        "e1:q13:announce_peer1:t2:".getBytes("UTF-8") ++ tid.toArray ++ "1:y1:qe".getBytes("UTF-8")

      "serialize message and send it to remote peer" in {
        agent ! Agent.Send(query, remote)
        expectMsgPF() {
          case Udp.Received(data, address) =>
            packet should equal(data.toArray)
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }
      
      "complete transaction and notify Controller after receiving network response" in {
        peer ! Udp.Send(Agent.serialize(new Response.AnnouncePeer(query.tid, Integer160.zero)), local)
        controllerInbox.receive(10.seconds) match {
          case Controller.Received(message, address) =>
            message match {
              case ap: Response.AnnouncePeer =>
                ap.id should be(Integer160.zero)
                ap.tid.toArray should equal(tid.toArray)
              case a: Any =>
                this.fail("Wrong message type " + a.getClass)
            }
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }

    }
    
    "command Send(PluginMessage) is issued" must {
    	
	      val tid = factory.next()
	      val message = new dht.message.Plugin(tid, Integer160.maxval, new PID(0), ByteString(Array[Byte]( '0', '1', '2', '3', '4'))) {}
	      val packet: Array[Byte] = "d1:pl20:".getBytes("UTF-8") ++ Integer160.maxval.toArray ++ "i0e".getBytes("UTF-8") ++
	    		  "5:01234e".getBytes("UTF-8") ++ "1:t2:".getBytes("UTF-8") ++ tid.toArray ++ "1:y1:pe".getBytes("UTF-8") 
	
	      "serialize message and send it to remote peer" in {
	        agent ! Agent.Send(message, remote)
	        expectMsgPF() {
	          case Udp.Received(data, address) =>
	            packet should equal(data.toArray)
	          case a: Any =>
	            this.fail("Wrong message type " + a.getClass)
	        }
	      }        
      }


    "network packet with invalid message structure sent" must {
      "respond with error message" in {
        val tid = factory.next()
        val packet: Array[Byte] = "d1:ad2:id20:".getBytes("UTF-8") ++ Integer160.maxval.toArray ++
          "e1:q4:ping1:t2:".getBytes("UTF-8") ++ tid.toArray ++ "1:y1:ze".getBytes("UTF-8")
        val error: Array[Byte] = "d1:eli204e14:Unknown methode1:t2:".getBytes("UTF-8") ++ tid.toArray ++
          "1:y1:ee".getBytes("UTF-8")
        peer ! Udp.Send(ByteString(packet), local)
        expectMsgPF() {
          case Udp.Received(dump, address) =>
            dump.toArray should equal(error)
          case a: Any =>
            this.fail("Wrong message type " + a.getClass)
        }
      }
    }
  }
}

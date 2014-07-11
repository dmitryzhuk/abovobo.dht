/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.controller

import java.net.{InetAddress, InetSocketAddress}

import akka.actor._
import akka.io.{Udp, IO}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.abovobo.dht.message.{Message, Response, Query}
import org.abovobo.dht.persistence.h2.{DataSource, Storage}
import org.abovobo.integer.Integer160
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.abovobo.dht.{TID, Agent, Node, Table}
import scala.concurrent.duration._
import org.abovobo.dht

class RemotePeer(val endpoint: InetSocketAddress) extends Actor with ActorLogging {

  import this.context.system

  override def preStart() = IO(Udp) ! Udp.Bind(self, this.endpoint)

  override def receive = {
    case Udp.Bound(l) =>
      this.log.info("Bound with local address {}", l)
      this.context.become(this.ready(this.sender()))
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

/**
 * Unit test for [[Controller]]
 */
class ControllerTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {


  def this() = this(ActorSystem("ControllerTest", ConfigFactory.parseString("akka.loglevel=debug")))

  private val ds = DataSource("jdbc:h2:~/db/dht;SCHEMA=ipv4")
  private val h2 = new Storage(ds.connection)
  private val reader = h2
  private val writer = h2

  val remote0 = new InetSocketAddress(InetAddress.getLoopbackAddress, 30000)
  val remote1 = new InetSocketAddress(InetAddress.getLoopbackAddress, 30001)

  val table = Inbox.create(this.system)
  val agent = Inbox.create(this.system)

  val controller = this.system.actorOf(Controller.props(List(remote0), reader, writer, agent.getRef(), table.getRef()))

  override def beforeAll() {
    println()
    println(this.self)
  }

  override def afterAll() {
    h2.close()
    ds.close()
    TestKit.shutdownActorSystem(this.system)
  }

  "Controller actor " when {
    "command Ping was issued" must {

      var tid: TID = null
      val id = Integer160.random

      "instruct Agent to send Ping message to remote peer" in {
        this.controller ! Controller.Ping(new Node(id, this.remote1))
        this.agent.receive(1.second) match {
          case Agent.Send(message, remote) =>
            message match {
              case ping: Query.Ping =>
                ping.id should equal(this.reader.id().get)
                tid = ping.tid
              case _ => this.fail("Invalid message type")
            }
            remote should equal(this.remote1)
        }
      }

      "and when received response from Agent, report this to Table and then respond to original requester" in {
        this.controller ! Controller.Received(new Response.Ping(tid, id), this.remote1)
        this.table.receive(1.second) match {
          case Table.Received(node, kind) =>
            node.id should equal(id)
            node.address should equal(this.remote1)
            kind should equal(Message.Kind.Response)
          case _ => this.fail("Invalid message type")
        }
        expectMsg(Controller.Pinged())
      }
    }

    "command AnnouncePeer was issued" must {

      var tid: TID = null
      val id = Integer160.random
      val infohash = Integer160.random

      "instruct Agent to send AnnouncePeer message to remote peer" in {
        this.controller ! Controller.AnnouncePeer(
          new Node(id, this.remote1), new dht.Token(2), infohash, 1, implied = true)
        this.agent.receive(1.second) match {
          case Agent.Send(message, remote) =>
            message match {
              case ap: Query.AnnouncePeer =>
                ap.id should equal(this.reader.id().get)
                tid = ap.tid
              case _ => this.fail("Invalid message type")
            }
            remote should equal(this.remote1)
        }
      }

      "and when received response from Agent, report this to Table and then respond to original requester" in {
        this.controller ! Controller.Received(new Response.AnnouncePeer(tid, id), this.remote1)
        this.table.receive(1.second) match {
          case Table.Received(node, kind) =>
            node.id should equal(id)
            node.address should equal(this.remote1)
            kind should equal(Message.Kind.Response)
          case _ => this.fail("Invalid message type")
        }
        expectMsg(Controller.PeerAnnounced())
      }
    }

    "command FindNode was issued" must {

      var tid: TID = null
      val id = Integer160.random
      val target = Integer160.random
      val seed = new Node(Integer160.random, remote0)

      "instruct Agent to send FindNode message to router(s)" in {
        this.controller ! Controller.FindNode(target)
        this.agent.receive(1.second) match {
          case Agent.Send(message, remote) =>
            message match {
              case fn: Query.FindNode =>
                fn.id should equal(this.reader.id().get)
                fn.target should equal(target)
                tid = fn.tid
              case _ => this.fail("Invalid message type")
            }
            remote should equal(this.remote0)
        }
      }

      "and when received response from Agent, continue iterations until FindNode exhausted" in {
        val distanceFromSeed = target ^ seed.id
        val lowerDistanceBound = distanceFromSeed / 1000
        val distanceRange = distanceFromSeed - lowerDistanceBound
        val ids = for (i <- 0 until 8) yield (lowerDistanceBound + Integer160.random % distanceRange) ^ target
        val nodes = ids.map(new Node(_, new InetSocketAddress(0)))

        this.controller ! Controller.Received(new Response.FindNode(tid, id, nodes), this.remote0)

        for (i <- 0 until 3) {
          this.agent.receive(1.second) match {
            case Agent.Send(message, remote) =>
              message match {
                case fn: Query.FindNode =>
                  fn.target should equal(target)
                  val distanceFromReporter = target ^ fn.id
                  val lowerDistanceBound = distanceFromReporter / 1000
                  val distanceRange = distanceFromReporter - lowerDistanceBound
                  val ids = for (i <- 0 until 8) yield (lowerDistanceBound + Integer160.random % distanceRange) ^ target
                  val nodes = ids.map(new Node(_, new InetSocketAddress(0)))
                  this.controller ! Controller.Received(new Response.FindNode(fn.tid, id, nodes), remote)
                case _ => this.fail("Invalid message type")
              }
          }
        }

        for (i <- 0 until 3) {
          this.agent.receive(1.second) match {
            case Agent.Send(message, remote) =>
              message match {
                case fn: Query.FindNode =>
                  fn.target should equal(target)
                  val distanceFromReporter = target ^ fn.id
                  val upperDistanceBound = distanceFromReporter + 1000
                  val distanceRange = upperDistanceBound - distanceFromReporter
                  val ids = for (i <- 0 until 8) yield (distanceFromReporter + Integer160.random % distanceRange) ^ target
                  val nodes = ids.map(new Node(_, new InetSocketAddress(0)))
                  this.controller ! Controller.Received(new Response.FindNode(fn.tid, id, nodes), remote)
                case _ => this.fail("Invalid message type")
              }
          }
        }

        for (i <- 0 until 8) {
          this.agent.receive(1.second) match {
            case Agent.Send(message, remote) =>
              message match {
                case fn: Query.FindNode =>
                  fn.target should equal(target)
                  val distanceFromReporter = target ^ fn.id
                  val upperDistanceBound = distanceFromReporter + 1000
                  val distanceRange = upperDistanceBound - distanceFromReporter
                  val ids = for (i <- 0 until 8) yield (distanceFromReporter + Integer160.random % distanceRange) ^ target
                  val nodes = ids.map(new Node(_, new InetSocketAddress(0)))
                  this.controller ! Controller.Received(new Response.FindNode(fn.tid, id, nodes), remote)
                case _ => this.fail("Invalid message type")
              }
          }
        }

        expectMsgPF(2.seconds) {
          case Controller.Found(nn, peers, tokens) =>
            nn should not be empty
            peers shouldBe empty
            tokens shouldBe empty
          case _ =>
            this.fail("Invalid message type")
        }
      }
    }

  }
}


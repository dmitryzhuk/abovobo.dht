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
import org.abovobo.dht.persistence.h2.{DataSource, Reader, Writer}
import org.abovobo.integer.Integer160
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.abovobo.dht._
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
  private val reader = new Reader(ds.connection)
  private val writer = new Writer(ds.connection)

  val remote0 = new InetSocketAddress(InetAddress.getLoopbackAddress, 30000)
  val remote1 = new InetSocketAddress(InetAddress.getLoopbackAddress, 30001)
  val dummy = new InetSocketAddress(0)

  val table = Inbox.create(this.system)
  val agent = Inbox.create(this.system)

  val controller = this.system.actorOf(Controller.props(List(remote0), reader, writer, table.getRef()))

  implicit val timeout: akka.util.Timeout = 5.seconds

  override def beforeAll() {
    this.writer.drop()

    println()
    println(this.self)

    controller.tell(Agent.Bound, agent.getRef())
  }

  override def afterAll() {
    this.reader.close()
    this.writer.close()
    this.ds.close()
    TestKit.shutdownActorSystem(this.system)
  }

  "Controller actor " when {

    "just created" must {
      "send Ready event to table" in {
        this.table.receive(10.seconds) match {
          case Controller.Ready => //
          case _ => this.fail("Invalid message type")
        }
      }
    }

    /** Commands */

    "command Ping was issued" must {

      var tid: TID = null
      val id = Integer160.random

      "instruct Agent to send Ping message to remote peer" in {
        this.controller ! Controller.Ping(new NodeInfo(id, this.remote1))
        this.agent.receive(10.seconds) match {
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
        this.controller ! Agent.Received(new Response.Ping(tid, id), this.remote1)
        this.table.receive(10.seconds) match {
          case Table.Received(node, kind) =>
            node.id should equal(id)
            node.address should equal(this.remote1)
            kind should equal(Message.Kind.Response)
          case m: Any => this.fail("Invalid message type: " + m.getClass.getName)
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
          new NodeInfo(id, this.remote1), new dht.Token(2), infohash, 1, implied = true)
        this.agent.receive(10.seconds) match {
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
        this.controller ! Agent.Received(new Response.AnnouncePeer(tid, id), this.remote1)
        this.table.receive(10.seconds) match {
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

      val id = Integer160.random
      val target = Integer160.random
      val zero = Integer160.zero
      val origin = target ^ zero

      var ck = 0
      var fk = 0

      // generates nodes with ids which are closer to target than given
      def closer() = {
        val ids = for (i <- 0 until 8) yield (origin - (8 * ck) - (i + 1)) ^ target
        ck += 1
        ids.map(new NodeInfo(_, this.dummy))
      }

      // generates nodes with ids which are more distant from target than given
      def farther() = {
        val ids = for (i <- 0 until 8) yield (origin + (8 * fk) + (i + 1)) ^ target
        fk += 1
        ids.map(new NodeInfo(_, this.dummy))
      }

      "instruct Agent to send FindNode message to router(s)" in {
        // issue a command to Controller
        this.controller ! Controller.FindNode(target)

        // receive the command that Controller has sent to network Agent
        val tid = this.agent.receive(10.second) match {
          case Agent.Send(message, remote) =>
            remote should equal(this.remote0)
            message match {
              case fn: Query.FindNode =>
                fn.id should equal(this.reader.id().get)
                fn.target should equal(target)
                fn.tid
              case _ => this.fail("Invalid message type")
            }
          case _ => this.fail("Invalid command type")
        }
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // -- at this point a finder corresponding to this recursion must have "Wait" state

        // notify Controller that response message with closer nodes
        this.controller ! Agent.Received(new Response.FindNode(tid, zero, closer()), this.remote0)

        // -- at this point a finder corresponding to this recursion must have "Continue" state
        // -- and produce "alpha" requests to a network agent as a next round of requests

        // receive all requests from the new round
        val q1: Traversable[Query.FindNode] = for (i <- 0 until 3) yield
          this.agent.receive(10.second) match {
            case Agent.Send(message, remote) =>
              message match {
                case fn: Query.FindNode =>
                  fn.target should equal(target)
                  fn
                case _ => this.fail("Invalid message type")
              }
            case _ => this.fail("Invalid command type")
          }
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // generate response for the first query with even closer nodes
        this.controller ! Agent.Received(new Response.FindNode(q1.head.tid, id, closer()), this.dummy)

        // -- at this point a finder must have "Continue" state and again
        // -- produce "alpha" more requests to a network agent as a next round of requests

        val q2: Traversable[Query.FindNode] = for (i <- 0 until 3) yield
          this.agent.receive(10.second) match {
            case Agent.Send(message, remote) =>
              message match {
                case fn: Query.FindNode =>
                  fn.target should equal(target)
                  fn
                case _ => this.fail("Invalid message type")
              }
            case _ => this.fail("Invalid command type")
          }
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // now get back to round 1:
        // complete second query with closer nodes which must cause 3 more queries
        // which we will complete with no closer nodes
        this.controller ! Agent.Received(new Response.FindNode(q1.drop(1).head.tid, id, closer()), this.dummy)
        val qx: Traversable[Query.FindNode] = for (i <- 0 until 3) yield {
          this.agent.receive(10.second) match {
            case Agent.Send(message, remote) =>
              message match {
                case fn: Query.FindNode =>
                  fn.target should equal(target)
                  fn
                case _ => this.fail("Invalid message type")
              }
            case _ => this.fail("Invalid command type")
          }
        }

        // and now complete the last query from round 1 which must not produce more queries
        this.controller ! Agent.Received(new Response.FindNode(q1.drop(2).head.tid, id, closer()), this.dummy)
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // now complete queries produced by the completion of second item from round 1
        qx.foreach { q =>
          this.controller ! Agent.Received(new Response.FindNode(q.tid, id, farther()), this.dummy)
        }
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // now complete second round with nodes which are not closer then already seen
        // these must not produce any additional queries after first 2 reports,
        // as corresponding Finder will be in Wait state and on the last report in the round
        // it should produce K (8) new queries to finalize lookup procedure
        q2.take(2).foreach { q =>
          this.controller ! Agent.Received(new Response.FindNode(q.tid, id, farther()), this.dummy)
          an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)
        }
        this.controller ! Agent.Received(new Response.FindNode(q2.last.tid, id, farther()), this.dummy)
        val q3: Traversable[Query.FindNode] = for (i <- 0 until 8) yield
          this.agent.receive(10.seconds) match {
            case Agent.Send(message, remote) =>
              message match {
                case fn: Query.FindNode =>
                  fn.target should equal(target)
                  fn
                case _ => this.fail("Invalid message type")
              }
          }

        // now complete all 8 new queries bringing no new nodes again which must cause
        // Controller to complete the whole procedure and send back Found message.
        q3.foreach { q =>
          this.controller ! Agent.Received(new Response.FindNode(q.tid, id, farther()), this.dummy)
          an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)
        }
        expectMsgPF(20.seconds) {
          case Controller.Found(nn, peers, tokens) =>
            nn should not be empty
            peers shouldBe empty
            tokens shouldBe empty
          case _ =>
            this.fail("Invalid message type")
        }
      }
    }

    "command GetPeers was issued" must {

      val id = Integer160.random
      val target = Integer160.random
      val zero = Integer160.zero
      val origin = target ^ zero
      val token = new dht.Token(2)

      var ck = 0
      var fk = 0

      // generates nodes with ids which are closer to target than given
      def closer() = {
        val ids = for (i <- 0 until 8) yield (origin - (8 * ck) - (i + 1)) ^ target
        ck += 1
        ids.map(new NodeInfo(_, this.dummy))
      }

      // generates nodes with ids which are more distant from target than given
      def farther() = {
        val ids = for (i <- 0 until 8) yield (origin + (8 * fk) + (i + 1)) ^ target
        fk += 1
        ids.map(new NodeInfo(_, this.dummy))
      }

      "instruct Agent to send GetPeers message to router(s)" in {
        // issue a command to Controller
        this.controller ! Controller.GetPeers(target)

        // receive the command that Controller has sent to network Agent
        val tid = this.agent.receive(1.second) match {
          case Agent.Send(message, remote) =>
            remote should equal(this.remote0)
            message match {
              case gp: Query.GetPeers =>
                gp.id should equal(this.reader.id().get)
                gp.infohash should equal(target)
                gp.tid
              case _ => this.fail("Invalid message type")
            }
          case _ => this.fail("Invalid command type")
        }
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // -- at this point a finder corresponding to this recursion must have "Wait" state

        // notify Controller that response message with closer nodes
        this.controller ! Agent.Received(new Response.GetPeersWithNodes(tid, id, token, closer()), this.remote0)

        // -- at this point a finder corresponding to this recursion must have "Continue" state
        // -- and produce "alpha" requests to a network agent as a next round of requests

        // receive all requests from the new round
        val q1: Traversable[Query.GetPeers] = for (i <- 0 until 3) yield
          this.agent.receive(1.second) match {
            case Agent.Send(message, remote) =>
              message match {
                case gp: Query.GetPeers =>
                  gp.infohash should equal(target)
                  gp
                case _ => this.fail("Invalid message type")
              }
            case _ => this.fail("Invalid command type")
          }
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // generate response for the first query with even closer nodes
        this.controller ! Agent.Received(new Response.GetPeersWithNodes(q1.head.tid, id, token, closer()), this.dummy)

        // -- at this point a finder must have "Continue" state and again
        // -- produce "alpha" more requests to a network agent as a next round of requests

        val q2: Traversable[Query.GetPeers] = for (i <- 0 until 3) yield
          this.agent.receive(1.second) match {
            case Agent.Send(message, remote) =>
              message match {
                case gp: Query.GetPeers =>
                  gp.infohash should equal(target)
                  gp
                case _ => this.fail("Invalid message type")
              }
            case _ => this.fail("Invalid command type")
          }
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // now get back to round 1:
        // complete second query with closer nodes which must cause 3 more queries
        // which we will complete with no closer nodes
        this.controller ! Agent.Received(
          new Response.GetPeersWithNodes(q1.drop(1).head.tid, id, token, closer()), this.dummy)
        val qx: Traversable[Query.GetPeers] = for (i <- 0 until 3) yield {
          this.agent.receive(1.second) match {
            case Agent.Send(message, remote) =>
              message match {
                case gp: Query.GetPeers =>
                  gp.infohash should equal(target)
                  gp
                case _ => this.fail("Invalid message type")
              }
            case _ => this.fail("Invalid command type")
          }
        }

        // and now complete the last query from round 1 which must not produce more queries
        this.controller ! Agent.Received(
          new Response.GetPeersWithNodes(q1.drop(2).head.tid, id, token, closer()), this.dummy)
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // now complete queries produced by the completion of second item from round 1
        qx.foreach { q =>
          this.controller ! Agent.Received(
            new Response.GetPeersWithValues(q.tid, id, token, Seq(new dht.Peer(0), new dht.Peer(1))), this.dummy)
        }
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // now complete second round with nodes which are not closer then already seen
        // these must not produce any additional queries after first 2 reports,
        // as corresponding Finder will be in Wait state and on the last report in the round
        // it should produce K (8) new queries to finalize lookup procedure
        q2.take(2).foreach { q =>
          this.controller ! Agent.Received(
            new Response.GetPeersWithNodes(q.tid, id, token, farther()), this.dummy)
          an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)
        }
        this.controller ! Agent.Received(
          new Response.GetPeersWithValues(q2.last.tid, id, token, Seq(new dht.Peer(0))), this.dummy)
        val q3: Traversable[Query.GetPeers] = for (i <- 0 until 8) yield
          this.agent.receive(10.seconds) match {
            case Agent.Send(message, remote) =>
              message match {
                case gp: Query.GetPeers =>
                  gp.infohash should equal(target)
                  gp
                case _ => this.fail("Invalid message type")
              }
          }

        // now complete all 8 new queries bringing no new nodes again which must cause
        // Controller to complete the whole procedure and send back Found message.
        q3.foreach { q =>
          this.controller ! Agent.Received(
            new Response.GetPeersWithValues(q.tid, id, token, Seq(new dht.Peer(1))), this.dummy)
          an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)
        }
        expectMsgPF(20.seconds) {
          case Controller.Found(nn, peers, tokens) =>
            nn should not be empty
            peers should not be empty
            tokens should not be empty
          case _ =>
            this.fail("Invalid message type")
        }
      }
    }

    /** Messages */

    "message Ping was received" must {
      "respond with own Ping response" in {
        val id = Integer160.random
        val tid = TIDFactory.random.next()
        val message = new Query.Ping(tid, id)
        this.controller ! Agent.Received(message, this.remote1)
        this.agent.receive(10.seconds) match {
          case Agent.Send(msg, address) =>
            msg match {
              case ping: Response.Ping =>
                msg.tid should equal(tid)
                address should equal(this.remote1)
              case _ => this.fail("Invalid message type")
            }
          case _ => this.fail("Invalid message")
        }
      }
    }

    "message FindNode was received" must {
      "respond with FindNode response" in {
        val id = Integer160.random
        val tid = TIDFactory.random.next()
        val message = new Query.FindNode(tid, id, Integer160.random)
        this.controller ! Agent.Received(message, this.remote1)
        this.agent.receive(10.seconds) match {
          case Agent.Send(msg, address) =>
            msg match {
              case fn: Response.FindNode =>
                msg.tid should equal(tid)
                address should equal(this.remote1)
                // XXX working with table with unknown content, no check here
                // fn.nodes should not be empty
              case _ => this.fail("Invalid message type: " + msg.getClass.getName)
            }
          case _ => this.fail("Invalid message")
        }
      }
    }

    var token: dht.Token = null

    "message GetPeers was received" must {
      "respond with GetPeersWithNodes assuming that no peers to respond with" in {
        val id = Integer160.random
        val tid = TIDFactory.random.next()
        val message = new Query.GetPeers(tid, id, Integer160.random)
        this.controller ! Agent.Received(message, this.remote1)
        this.agent.receive(10.seconds) match {
          case Agent.Send(msg, address) =>
            msg match {
              case gp: Response.GetPeersWithNodes =>
                msg.tid should equal(tid)
                address should equal(this.remote1)
                // XXX working with table with unknown content, no check here
                // fn.nodes should not be empty
                gp.token should not be empty
                token = gp.token
              case _ => this.fail("Invalid message type: " + msg.getClass.getName)
            }
          case _ => this.fail("Invalid message")
        }
      }
    }

    "message AnnouncePeer was received" must {
      "respond with Announced response" in {
        val id = Integer160.random
        val tid = TIDFactory.random.next()
        val message = new Query.AnnouncePeer(tid, id, Integer160.random, 0, token, implied = false)
        this.controller ! Agent.Received(message, this.remote1)
        this.agent.receive(10.seconds) match {
          case Agent.Send(msg, address) =>
            msg match {
              case ap: Response.AnnouncePeer =>
                msg.tid should equal(tid)
                address should equal(this.remote1)
              case _ => this.fail("Invalid message type: " + msg.getClass.getName)
            }
          case _ => this.fail("Invalid message")
        }
      }
    }

  }
}


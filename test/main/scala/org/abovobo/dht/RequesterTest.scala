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

import java.net.InetSocketAddress

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.abovobo.dht
import org.abovobo.dht.message.{Message, Query, Response}
import org.abovobo.dht.persistence.h2.{DynamicallyConnectedStorage, DataSource}
import org.abovobo.integer.Integer160
import org.abovobo.jdbc.Closer._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

/**
 * Unit test for [[Requester]]
 */
class RequesterTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {


  def this() = this(ActorSystem("RequesterTest", ConfigFactory.parseString("akka.loglevel=debug")))

  val ds = DataSource("jdbc:h2:~/db/requester-test")
  val storage = new DynamicallyConnectedStorage(this.ds)
  val table = Inbox.create(this.system)
  val agent = Inbox.create(this.system)
  val dummy = new InetSocketAddress(0)

  val requester: ActorRef = this.system.actorOf(Requester.props(List(dummy), storage, table.getRef()))

  //implicit val timeout: akka.util.Timeout = 5.seconds

  override def beforeAll() {

    // drop everything in the beginning and create working schema
    using(this.ds.connection) { c =>
      using(c.createStatement()) { s =>
        s.execute("drop all objects")
        s.execute("create schema ipv4")
        c.commit()
      }
    }

    // set working schema for the storage
    this.storage.setSchema("ipv4")

    // create common tables
    this.storage.execute("/tables-common.sql")

    // create tables specific to IPv4 protocol which will be used in test
    this.storage.execute("/tables-ipv4.sql")

    // initialize storage with random self ID
    this.storage.transaction(this.storage.id(Integer160.random))
  }

  override def afterAll() {
    this.storage.close()
    this.ds.close()
    TestKit.shutdownActorSystem(this.system)
  }

  "Requester actor " when {

    "just created" must {
      "send Ready event to table" in {
        this.requester.tell(Agent.Bound, this.agent.getRef())
        this.table.receive(10.seconds) match {
          case Requester.Ready => //
          case _ => this.fail("Invalid message type")
        }
      }
    }

    /** Commands */

    "command Ping was issued" must {

      var tid: TID = null
      val id = Integer160.random

      "instruct Agent to send Ping message to remote peer" in {
        this.requester ! Requester.Ping(new NodeInfo(id, this.dummy))
        this.agent.receive(10.seconds) match {
          case Agent.Send(message, remote) =>
            message match {
              case ping: Query.Ping =>
                ping.id should equal(this.storage.id().get)
                tid = ping.tid
              case _ => this.fail("Invalid message type")
            }
            remote should equal(this.dummy)
        }
      }

      "and when received response from Agent, report this to Table and then respond to original requester" in {
        this.requester ! Agent.Received(new Response.Ping(tid, id), this.dummy)
        this.table.receive(10.seconds) match {
          case Table.Received(node, kind) =>
            node.id should equal(id)
            node.address should equal(this.dummy)
            kind should equal(Message.Kind.Response)
          case m: Any => this.fail("Invalid message type: " + m.getClass.getName)
        }
        expectMsg(Requester.Pinged())
      }
    }

    "command AnnouncePeer was issued" must {

      var tid: TID = null
      val id = Integer160.random
      val infohash = Integer160.random

      "instruct Agent to send AnnouncePeer message to remote peer" in {
        this.requester ! Requester.AnnouncePeer(
          new NodeInfo(id, this.dummy), new dht.Token(2), infohash, 1, implied = true)
        this.agent.receive(10.seconds) match {
          case Agent.Send(message, remote) =>
            message match {
              case ap: Query.AnnouncePeer =>
                ap.id should equal(this.storage.id().get)
                tid = ap.tid
              case _ => this.fail("Invalid message type")
            }
            remote should equal(this.dummy)
        }
      }

      "and when received response from Agent, report this to Table and then respond to original requester" in {
        this.requester ! Agent.Received(new Response.AnnouncePeer(tid, id), this.dummy)
        this.table.receive(10.seconds) match {
          case Table.Received(node, kind) =>
            node.id should equal(id)
            node.address should equal(this.dummy)
            kind should equal(Message.Kind.Response)
          case _ => this.fail("Invalid message type")
        }
        expectMsgType[Requester.PeerAnnounced]
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
        // issue a command to Requester
        this.requester ! Requester.FindNode(target)

        // receive the command that Requester has sent to network Agent
        val tid = this.agent.receive(10.second) match {
          case Agent.Send(message, remote) =>
            remote should equal(this.dummy)
            message match {
              case fn: Query.FindNode =>
                fn.id should equal(this.storage.id().get)
                fn.target should equal(target)
                fn.tid
              case _ => this.fail("Invalid message type")
            }
          case _ => this.fail("Invalid command type")
        }
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // -- at this point a finder corresponding to this recursion must have "Wait" state

        // notify Requester that response message with closer nodes
        this.requester ! Agent.Received(new Response.FindNode(tid, zero, closer()), this.dummy)

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
        this.requester ! Agent.Received(new Response.FindNode(q1.head.tid, id, closer()), this.dummy)

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
        this.requester ! Agent.Received(new Response.FindNode(q1.drop(1).head.tid, id, closer()), this.dummy)
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
        this.requester ! Agent.Received(new Response.FindNode(q1.drop(2).head.tid, id, closer()), this.dummy)
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // now complete queries produced by the completion of second item from round 1
        qx.foreach { q =>
          this.requester ! Agent.Received(new Response.FindNode(q.tid, id, farther()), this.dummy)
        }
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // now complete second round with nodes which are not closer then already seen
        // these must not produce any additional queries after first 2 reports,
        // as corresponding Finder will be in Wait state and on the last report in the round
        // it should produce K (8) new queries to finalize lookup procedure
        q2.take(2).foreach { q =>
          this.requester ! Agent.Received(new Response.FindNode(q.tid, id, farther()), this.dummy)
          an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)
        }
        this.requester ! Agent.Received(new Response.FindNode(q2.last.tid, id, farther()), this.dummy)
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
        // Requester to complete the whole procedure and send back Found message.
        q3.foreach { q =>
          this.requester ! Agent.Received(new Response.FindNode(q.tid, id, farther()), this.dummy)
          an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)
        }
        expectMsgPF(20.seconds) {
          case Requester.Found(target, nn, peers, tokens) =>
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
        // issue a command to Requester
        this.requester ! Requester.GetPeers(target)

        // receive the command that Requester has sent to network Agent
        val tid = this.agent.receive(1.second) match {
          case Agent.Send(message, remote) =>
            remote should equal(this.dummy)
            message match {
              case gp: Query.GetPeers =>
                gp.id should equal(this.storage.id().get)
                gp.infohash should equal(target)
                gp.tid
              case _ => this.fail("Invalid message type")
            }
          case _ => this.fail("Invalid command type")
        }
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // -- at this point a finder corresponding to this recursion must have "Wait" state

        // notify Requester that response message with closer nodes
        this.requester ! Agent.Received(new Response.GetPeersWithNodes(tid, id, token, closer()), this.dummy)

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
        this.requester ! Agent.Received(new Response.GetPeersWithNodes(q1.head.tid, id, token, closer()), this.dummy)

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
        this.requester ! Agent.Received(
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
        this.requester ! Agent.Received(
          new Response.GetPeersWithNodes(q1.drop(2).head.tid, id, token, closer()), this.dummy)
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // now complete queries produced by the completion of second item from round 1
        qx.foreach { q =>
          this.requester ! Agent.Received(
            new Response.GetPeersWithValues(q.tid, id, token, Seq(new dht.Peer(0), new dht.Peer(1))), this.dummy)
        }
        an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)

        // now complete second round with nodes which are not closer then already seen
        // these must not produce any additional queries after first 2 reports,
        // as corresponding Finder will be in Wait state and on the last report in the round
        // it should produce K (8) new queries to finalize lookup procedure
        q2.take(2).foreach { q =>
          this.requester ! Agent.Received(
            new Response.GetPeersWithNodes(q.tid, id, token, farther()), this.dummy)
          an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)
        }
        this.requester ! Agent.Received(
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
        // Requester to complete the whole procedure and send back Found message.
        q3.foreach { q =>
          this.requester ! Agent.Received(
            new Response.GetPeersWithValues(q.tid, id, token, Seq(new dht.Peer(1))), this.dummy)
          an [java.util.concurrent.TimeoutException] should be thrownBy this.agent.receive(1.second)
        }
        expectMsgPF(20.seconds) {
          case Requester.Found(target, nn, peers, tokens) =>
            nn should not be empty
            peers should not be empty
            tokens should not be empty
          case _ =>
            this.fail("Invalid message type")
        }
      }
    }

  }
}


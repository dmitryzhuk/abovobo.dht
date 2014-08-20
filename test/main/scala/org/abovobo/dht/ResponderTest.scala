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

import akka.actor.{ActorSystem, Inbox}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.abovobo.dht
import org.abovobo.dht.message.{Query, Response}
import org.abovobo.dht.persistence.h2.{DataSource, DynamicallyConnectedStorage}
import org.abovobo.integer.Integer160
import org.abovobo.jdbc.Closer._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/** Tests [[Responder]] actor. */
class ResponderTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("ResponderTest", ConfigFactory.parseString("akka.loglevel=debug")))

  val ds = DataSource("jdbc:h2:~/db/responder-test")
  val storage = new DynamicallyConnectedStorage(this.ds)
  val table = Inbox.create(this.system)
  val remote = new InetSocketAddress(InetAddress.getLoopbackAddress, 30000)
  val responder = this.system.actorOf(Responder.props(this.storage, this.table.getRef()))

  override def beforeAll() = {

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

  override def afterAll() = {
    this.storage.close()
    this.ds.close()
    TestKit.shutdownActorSystem(this.system)
  }


  "Responder actor" when {

    /** Messages */

    "message Ping was received" must {
      "respond with own Ping response" in {
        val id = Integer160.random
        val tid = TIDFactory.random.next()
        val message = new Query.Ping(tid, id)
        this.responder ! Agent.Received(message, this.remote)
        expectMsgPF() {
          case Agent.Send(msg, address) =>
            msg match {
              case ping: Response.Ping =>
                msg.tid should equal(tid)
                address should equal(this.remote)
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
        this.responder ! Agent.Received(message, this.remote)
        expectMsgPF() {
          case Agent.Send(msg, address) =>
            msg match {
              case fn: Response.FindNode =>
                msg.tid should equal(tid)
                address should equal(this.remote)
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
        this.responder ! Agent.Received(message, this.remote)
        expectMsgPF() {
          case Agent.Send(msg, address) =>
            msg match {
              case gp: Response.GetPeersWithNodes =>
                msg.tid should equal(tid)
                address should equal(this.remote)
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
        this.responder ! Agent.Received(message, this.remote)
        expectMsgPF() {
          case Agent.Send(msg, address) =>
            msg match {
              case ap: Response.AnnouncePeer =>
                msg.tid should equal(tid)
                address should equal(this.remote)
              case _ => this.fail("Invalid message type: " + msg.getClass.getName)
            }
          case _ => this.fail("Invalid message")
        }
      }
    }
  }
}

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

import akka.actor.{ActorSystem, ActorLogging, Actor}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.abovobo.dht.persistence._
import com.typesafe.config.ConfigFactory

//import org.abovobo.dht.persistence.H2DataSource

/**
 * Unit test for [[org.abovobo.dht.Controller]]
 */
class ControllerTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("ControllerTest", ConfigFactory.parseString("akka.loglevel=debug")))

  private val ds = H2DataSource("jdbc:h2:~/db/dht;SCHEMA=ipv4")
  private val h2 = new H2Storage(ds.connection)
  private val reader = h2
  private val writer = h2

  val controller = this.system.actorOf(Controller.props(Nil, reader, writer))

  override def afterAll() {
    h2.close()
    TestKit.shutdownActorSystem(this.system)
  }
}


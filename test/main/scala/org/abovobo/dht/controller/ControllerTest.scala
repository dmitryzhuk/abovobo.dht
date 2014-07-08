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

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.abovobo.dht.persistence.h2.{DataSource, Storage}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

//import org.abovobo.dht.persistence.h2.H2DataSource

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

  val controller = this.system.actorOf(Controller.props(Nil, reader, writer, null, null))

  override def afterAll() {
    h2.close()
    TestKit.shutdownActorSystem(this.system)
  }
}


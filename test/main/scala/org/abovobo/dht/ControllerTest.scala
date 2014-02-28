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
import org.abovobo.dht.persistence.{Writer, Reader, Storage, H2Storage}

/**
 * Unit test for [[org.abovobo.dht.Controller]]
 */
class ControllerTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("ControllerTest"))
  private val h2 = new H2Storage("jdbc:h2:~/db/dht;SCHEMA=ipv4")

  val storage: Storage = this.h2
  val reader: Reader = this.h2
  val writer: Writer = this.h2

  val controller = this.system.actorOf(Controller.props(Nil, reader, writer))

}


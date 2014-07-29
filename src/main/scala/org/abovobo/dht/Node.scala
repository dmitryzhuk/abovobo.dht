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

import scala.concurrent.duration._

import akka.actor.{Actor, ActorRef}
import org.abovobo.dht.controller.Controller
import org.abovobo.dht.persistence.{Storage, Writer, Reader}
import org.abovobo.dht.persistence.h2

/**
 * This class represents general wrapper over major components of DHT node: Table, Agent and Controller.
 * Note, that enclosed actors only exist while Node actor exists.
 *
 * @param storage   An instance of [[Storage]] class to be used by enclosed actors.
 * @param endpoint  An endpoint to listen for network messages at.
 * @param routers   Collection of initial routers used to start building DHT.
 * @param id        Optional parameter allowing to specify some unique identifier for this node.
 */
class Node(val storage: Storage,
           val endpoint: InetSocketAddress,
           val routers: Traversable[InetSocketAddress],
           private val id: Long = 0L)
  extends Actor {

  import this.context.system

  private val readerC = new h2.Reader(this.storage.connection)
  private val writerC = new h2.Writer(this.storage.connection)
  private val readerT = new h2.Reader(this.storage.connection)
  private val writerT = new h2.Writer(this.storage.connection)

  /*
  val controller = system.actorOf(
    Controller.props(this.routers, this.readerC, this.writerC, this.agent, this.table),
    "controller" + this.id)

  val table = system.actorOf(Table.props(this.readerT, this.writerT, this.controller), "table" + this.id)

  val agent = system.actorOf(Agent.props(this.endpoint, 3.seconds, this.controller), "agent" + this.id)
  */

  override def preStart() = {
    // --
  }

  override def postStop() = {
    //this.reader.close()
    //this.writer.close()
  }

  /**
   * @inheritdoc
   *
   * Handles received messages.
   */
  override def receive = {
    case _ =>
  }


}

/** Accompanying object */
object Node {

  def apply(reader: Reader, writer: Writer, endpoint: InetSocketAddress, routers: Traversable[InetSocketAddress]): Node = {
    null
  }

  def apply(storage: Storage, endpoint: InetSocketAddress, routers: Traversable[InetSocketAddress]): Node = {
    //this.apply(new h2.Reader(storage.connection))
    null
  }

}

/*
import java.net.InetSocketAddress
import akka.actor.actorRef2Scala
import org.abovobo.dht.controller.Controller
import org.abovobo.dht.persistence.Storage
import org.abovobo.dht.persistence.h2.{Reader, Writer}
import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorSystem
import java.net.InetAddress
import akka.actor.PoisonPill

class DhtNode(endpoint: InetSocketAddress, routers: List[InetSocketAddress]) extends Actor {
  
  val dataSource = null //H2DataSource.open("~/db/dht-" + self.path.name, true)
  
  val storageC: Storage = null //H2Storage(dataSource.getConnection) // controller
  val storageT: Storage = null //H2Storage(dataSource.getConnection) // table
  val storageD: Reader = null //H2Storage(dataSource.getConnection) // node (self)
    
  val controller = null //this.context.actorOf(Controller.props(routers, storageC, storageC, null, null), "controller")
  val agent = this.context.actorOf(Agent.props(endpoint, 10.seconds, controller), "agent")
  
  Thread.sleep(300) // Agent needs time to bind a socket and become an agent

  val table = null //this.context.actorOf(Table.props(storageT, storageT, controller), "table")
    
  override def postStop() {
    super.postStop()
    storageC.close()
    storageT.close()
    storageD.close()
    // dataSource will be disposed when all connections are closed
  }
  
  override def receive() = {
    case DhtNode.Stop => self ! PoisonPill
    case DhtNode.Describe => {
      if (storageD.id.isEmpty) {
        this.context.system.scheduler.scheduleOnce(250.milliseconds, self, DhtNode.Describe)(this.context.system.dispatcher, sender())
      } else {
        sender ! DhtNode.NodeInfo(new NodeInfo(storageD.id.get, endpoint), controller, storageD.nodes)
      }
    }
    case msg => //this.controller.forward(msg)
  }
}

object DhtNode {
  object Stop
  object Describe
  
  case class NodeInfo(self: NodeInfo, controller: ActorRef, nodes: Traversable[NodeInfo])
  
  def props(endpoint: InetSocketAddress, routers: List[InetSocketAddress] = List()) = 
    Props(classOf[DhtNode], endpoint, routers)
    
  def createNode(system: ActorSystem, endpoint: InetSocketAddress, routers: List[InetSocketAddress] = List()): ActorRef = {
    system.actorOf(DhtNode.props(endpoint, routers), "NodeInfo-" + endpoint.getPort)
  }
    
  def spawnNodes[A](system: ActorSystem, portBase: Int, count: Int)(f: (InetSocketAddress, ActorRef) => A): Seq[A] = {
    
    val routerEp = new InetSocketAddress(InetAddress.getLocalHost, portBase)
        
    val router = createNode(system, routerEp)
    
    val eps = for (i <- 1 until count) yield new InetSocketAddress(InetAddress.getLocalHost, portBase + i)

    Seq(f(routerEp, router)) ++ eps.map { ep => 
      f(ep, createNode(system, ep, List(routerEp)))
    }
  }
}
*/
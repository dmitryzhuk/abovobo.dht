package org.abovobo.dht

import akka.actor.Actor
import java.net.InetSocketAddress
import akka.actor.actorRef2Scala
import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorSystem
import java.net.InetAddress
import akka.actor.PoisonPill
import org.abovobo.dht.persistence.h2.DataUtils
import org.abovobo.dht.persistence.h2.Storage

class DhtNode(endpoint: InetSocketAddress, routers: List[InetSocketAddress]) extends Actor {
  
  val dataSource = DataUtils.openDatabase("~/db/dht-" + self.path.name, true)
  
  val storageC = new Storage(dataSource.connection) // controller
  val storageT = new Storage(dataSource.connection) // table
  val storageD = new Storage(dataSource.connection) // node (self)
    
  val controller = this.context.actorOf(Controller.props(routers, storageC, storageC), "controller")
  val agent = this.context.actorOf(Agent.props(endpoint, 10.seconds, controller), "agent")
  
  Thread.sleep(300) // Agent needs time to bind a socket and become an agent

  val table = this.context.actorOf(Table.props(storageT, storageT, controller), "table")
    
  override def postStop() {
    super.postStop()
    storageC.close()
    storageT.close()
    storageD.close()
    dataSource.close()
  }
  
  override def receive() = {
    case DhtNode.Stop => self ! PoisonPill
    case DhtNode.Describe => {
      if (storageD.id.isEmpty) {
        this.context.system.scheduler.scheduleOnce(250 milliseconds, self, DhtNode.Describe)(this.context.system.dispatcher, sender())
      } else {
        sender ! DhtNode.NodeInfo(new Node(storageD.id.get, endpoint), controller, storageD.nodes)        
      }
    }
    case msg => this.controller.forward(msg)
  }
}

object DhtNode {
  object Stop
  object Describe
  
  case class NodeInfo(self: Node, controller: ActorRef, nodes: Traversable[Node])
  
  def props(endpoint: InetSocketAddress, routers: List[InetSocketAddress] = List()) = 
    Props(classOf[DhtNode], endpoint, routers)
    
  def createNode(system: ActorSystem, endpoint: InetSocketAddress, routers: List[InetSocketAddress] = List()): ActorRef = {
    system.actorOf(DhtNode.props(endpoint, routers), "Node-" + endpoint.getPort)
  }
    
  def spawnNodes[A](system: ActorSystem, portBase: Int, count: Int, routers: List[InetSocketAddress])(f: (InetSocketAddress, ActorRef) => A): Seq[A] = {
    (1 until count) map { i =>
      val ep = new InetSocketAddress(InetAddress.getLocalHost, portBase + i)
      f(ep, createNode(system, ep, routers))
    }
  }
}

package org.abovobo.dht

import akka.actor.Actor
import java.net.InetSocketAddress
import org.abovobo.dht.persistence.H2Storage
import akka.actor.actorRef2Scala
import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorSystem
import java.net.InetAddress
import akka.actor.PoisonPill
import org.abovobo.dht.persistence.H2DataSource

class DhtNode(endpoint: InetSocketAddress, routers: List[InetSocketAddress]) extends Actor {
  
  val dataSource = H2DataSource.open("~/db/dht-" + self.path.name + ";TRACE_LEVEL_FILE=2", true)
  
  val storageC: H2Storage = H2Storage(dataSource.getConnection) // controller
  val storageT: H2Storage = H2Storage(dataSource.getConnection) // table
  val storageD: H2Storage = H2Storage(dataSource.getConnection) // node (self)
    
  val controller = this.context.actorOf(Controller.props(routers, storageC, storageC), "controller")
  val agent = this.context.actorOf(Agent.props(endpoint, 10.seconds, controller), "agent")
  
  Thread.sleep(300) // Agent needs time to bind a socket and become an agent

  val table = this.context.actorOf(Table.props(storageT, storageT, controller), "table")
    
  override def postStop() {
    super.postStop()
    storageC.close()
    storageT.close()
    storageD.close()
    // dataSource will be disposed when all connections are closed
  }
  
  override def receive() = {
    case DhtNode.Stop => self ! PoisonPill
    case DhtNode.Describe => sender ! DhtNode.NodeInfo(new Node(storageD.id.get, endpoint), controller, storageD.nodes)
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
    
  def spawnNodes(system: ActorSystem, portBase: Int, count: Int): Seq[(InetSocketAddress, ActorRef)] = {
    
    val routerEp = new InetSocketAddress(InetAddress.getLocalHost, portBase)
        
    val router = createNode(system, routerEp)
    
    val eps = for (i <- 1 until count) yield new InetSocketAddress(InetAddress.getLocalHost, portBase + i)

    Seq(routerEp -> router) ++ eps.map { ep => 
      Thread.sleep(1 * 1000)
      ep -> createNode(system, ep, List(routerEp)) 
    }
  }
}
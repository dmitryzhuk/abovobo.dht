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
import java.nio.file.Path

class DhtNode(homeDir: Path, endpoint: InetSocketAddress, routers: List[InetSocketAddress]) extends Actor {  
  homeDir.toFile.mkdirs
  
  val dataSource = DataUtils.openDatabase(homeDir.resolve("dht").toString, true)
  
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
  
  def props(homeDir: Path, endpoint: InetSocketAddress, routers: List[InetSocketAddress] = List()) = 
    Props(classOf[DhtNode], homeDir, endpoint, routers)
    
  def createNode(homeDir: Path, system: ActorSystem, endpoint: InetSocketAddress, routers: List[InetSocketAddress] = List()): ActorRef = {
    system.actorOf(DhtNode.props(homeDir, endpoint, routers), "Node-" + endpoint.getPort)
  }    
}

  package org.abovobo.dht

  import akka.actor.Actor
  import java.net.InetSocketAddress
  import org.abovobo.dht.persistence.H2Storage
  import org.abovobo.dht.persistence.Storage
  import org.abovobo.dht.persistence.Reader
  import org.abovobo.dht.persistence.Writer
  import akka.actor.actorRef2Scala
  import org.abovobo.integer.Integer160
  import scala.concurrent.duration._
  import akka.actor.ActorRef
  import akka.actor.Props
  import akka.actor.ActorSystem
  import java.net.InetAddress
  import akka.actor.PoisonPill

class DhtNode(endpoint: InetSocketAddress, routers: List[InetSocketAddress]) extends Actor {
  
  val h2 = H2Storage.open("~/db/dht-" + self.path.name, true) 
  val storage: Storage = h2
  val reader: Reader = h2
  val writer: Writer = h2
    
  val controller = this.context.actorOf(Controller.props(routers, reader, writer), "controller")

  val agent = this.context.actorOf(Agent.props(endpoint, 10 seconds, controller), "agent")
  
  Thread.sleep(300) // Agent needs time to bind a socket and become an agent

  val table = this.context.actorOf(Table.props(reader, writer, controller), "table")    
  
  Thread.sleep(100) // allow to generate id
  
  override def postStop() {
    super.postStop()
    h2.close()
  }
  
  override def receive() = {
    case DhtNode.Stop => self ! PoisonPill
    case DhtNode.Describe => sender ! DhtNode.NodeInfo(new Node(reader.id.get, endpoint), controller, reader.nodes)
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
      Thread.sleep(1000)
      ep -> createNode(system, ep, List(routerEp)) 
    }
  }
}
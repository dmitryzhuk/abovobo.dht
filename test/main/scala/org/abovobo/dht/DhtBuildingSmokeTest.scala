package org.abovobo.dht

import java.net.InetSocketAddress
import java.net.Inet4Address
import java.net.InetAddress
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorLogging
import com.typesafe.config.Config
import com.typesafe.config.impl.SimpleConfig
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._
import akka.actor.Inbox
import akka.actor.Props
import akka.actor.ActorDSL._
import org.abovobo.dht.persistence.H2Storage
import org.abovobo.dht.persistence.Storage
import org.abovobo.dht.persistence.Reader
import org.abovobo.dht.persistence.Writer
import org.abovobo.arm.Disposable
import akka.actor.ActorRef
import org.abovobo.integer.Integer160
import akka.actor.ActorDSL._
import akka.actor.ActorSystem
import scala.concurrent.duration._
import java.util.concurrent.TimeoutException
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import org.abovobo.dht.Controller.PutPlugin
import akka.pattern.ask

import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._


object DhtBuildingSmokeTest extends App {
  val systemConfig = ConfigFactory.parseMap(Map(
      "akka.log-dead-letters" -> "true", 
      "akka.actor.debug.lifecycle" -> false,
      "akka.loglevel" -> "debug",
      
    "akka.actor.debug.receive" -> true,
    "akka.actor.debug.send" -> true,

    "akka.actor.debug.unhandled" -> true))
    
    
  def localEndpoint(ordinal: Int) = new InetSocketAddress(InetAddress.getLocalHost, 20000 + ordinal)

//  case class NodeSystem(ordinal: Int, table: ActorRef, agent: ActorRef, controller: ActorRef, system: ActorSystem, storage: H2Storage) extends Disposable {
//    val endpoint: InetSocketAddress = localEndpoint(ordinal)
//    def dispose() {
//        //system.shutdown()
//        //system.awaitTermination()
//        storage.close()     
//    }
//  }
  

  
  class NodeSystem(ordinal: Int, routers: List[InetSocketAddress]) extends Actor {
    
    val h2 = H2Storage.open("~/db/dht-" + ordinal, true) 
    val storage: Storage = h2
    val reader: Reader = h2
    val writer: Writer = h2
    
    val endpoint = localEndpoint(ordinal)
    
    def name(s: String) = s

    val controller = this.context.actorOf(Controller.props(routers, reader, writer), name("controller"))

    val agent = this.context.actorOf(Agent.props(endpoint, 10 seconds, controller), name("agent"))
    
    Thread.sleep(250) // Agent needs time to bind a socket and become an agent

    val table = this.context.actorOf(Table.props(reader, writer, controller), name("table"))    
    
    override def receive() = {
      case NodeSystem.Dispose => h2.close()
      case NodeSystem.Describe => sender ! NodeSystem.NodeInfo(reader.id.get, endpoint, reader.nodes)
      case msg => this.controller.forward(msg)
    }
  }
  
  object NodeSystem {
    object Dispose
    object Describe
    case class NodeInfo(id: Integer160, endpoint: InetSocketAddress, nodes: Traversable[Node])
  }
  

  val system = ActorSystem("TestSystem", systemConfig)
    
  def createNode(ordinal: Int, routers: List[InetSocketAddress] = List()): ActorRef = {
    system.actorOf(Props(classOf[NodeSystem], ordinal, routers), "Node-" + ordinal.toString)
  }
  
  val timeoutDuration = 10 seconds
  implicit val timeout = Timeout(timeoutDuration)


  val routerEndpoint = localEndpoint(0)
  val router = createNode(0)

  val nodes = List(router) ++ (for (i <- 1 to 30) yield {
    Thread.sleep(100)
    createNode(i, List(routerEndpoint))
  })
  
  println("---------- waiting -------- ")

  Thread.sleep(15 * 1000)


  for (i <- 1 to 20) {
    println("---------- tables -------- ")
    nodes.foreach { node =>
      
      val nl = Await.result(node ? NodeSystem.Describe, timeoutDuration).asInstanceOf[NodeSystem.NodeInfo]
      
      println("dht table for: " + nl.id + "@" + nl.endpoint)
      nl.nodes.foreach { entry => println("\t" + entry)}
    }    

    Thread.sleep(30 * 1000)
    
    println("---------- FIND NODE -------- ")
    
    nodes.foreach { n =>
      n ! Controller.FindNode(Integer160.random)
      Thread.sleep(2 * 1000)
    }
  }

  nodes.foreach { n => n ! NodeSystem.Dispose }

  Thread.sleep(3 * 1000)
  
  system.shutdown()
  
  Thread.sleep(1 * 1000)
}
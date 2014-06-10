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



object DhtBuildingSmokeTest extends App {
  val systemConfig = ConfigFactory.parseMap(Map(
      "akka.log-dead-letters" -> "true", 
      "akka.actor.debug.lifecycle" -> false,
      "akka.loglevel" -> "debug",
      
    "akka.actor.debug.receive" -> true,
    "akka.actor.debug.send" -> true,

    "akka.actor.debug.unhandled" -> true))
    
    
  def localEndpoint(ordinal: Int) = new InetSocketAddress(InetAddress.getLocalHost, 20000 + ordinal)

  case class NodeSystem(ordinal: Int, table: ActorRef, agent: ActorRef, controller: ActorRef, system: ActorSystem, storage: H2Storage) extends Disposable {
    val endpoint: InetSocketAddress = localEndpoint(ordinal)
    def dispose() {
        system.shutdown()
        system.awaitTermination()
        storage.close()     
    }
  }
  
    
  def createNode(ordinal: Int, routers: List[InetSocketAddress] = List()): NodeSystem = {
      val h2 = H2Storage.open("~/db/dht-" + ordinal, true) 

    val storage: Storage = h2
    val reader: Reader = h2
    val writer: Writer = h2
      
    val system = ActorSystem("TestSystem-" + ordinal, systemConfig)

    val controller = system.actorOf(Controller.props(routers, reader, writer), "controller")

    val agent = system.actorOf(Agent.props(localEndpoint(ordinal), 10 seconds, controller), "agent")
    
    Thread.sleep(250) // Agent needs time to bind a socket and become an agent

    val table = system.actorOf(Table.props(reader, writer, controller), "table")    
  
    NodeSystem(ordinal, table, agent, controller, system, h2)
  }

  val router = createNode(0)

  val nodes = List(router) ++ (for (i <- 1 to 3) yield {
    Thread.sleep(3 * 1000)
    createNode(i, List(router.endpoint))
  })
  
  println("---------- waiting -------- ")

  Thread.sleep(7 * 1000)


  for (i <- 1 to 20) {
    println("---------- tables -------- ")
    nodes.foreach { node =>
      println("dht table for: " + node.storage.id + "@" + node.endpoint)
      node.storage.nodes.foreach { entry => println("\t" + entry)}
    }    

    Thread.sleep(60 * 1000)
  }
  

  nodes.foreach(_.dispose)
  
  Thread.sleep(5 * 1000)
}
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
import org.abovobo.dht.DhtNode


object DhtBuildingSmokeTest extends App {
  val systemConfig = ConfigFactory.parseMap(Map(
      "akka.log-dead-letters" -> "true", 
      "akka.actor.debug.lifecycle" -> false,
      "akka.loglevel" -> "debug",
      
    "akka.actor.debug.receive" -> true,
    "akka.actor.debug.send" -> true,

    "akka.actor.debug.unhandled" -> true))
    
    

  val system = ActorSystem("TestSystem", systemConfig)

  
  val timeoutDuration = 10 seconds
  implicit val timeout = Timeout(timeoutDuration)

  val nodes = DhtNode.spawnNodes(system, 20000, 30)
  
  println("---------- waiting -------- ")

  Thread.sleep(15 * 1000)


  for (i <- 1 to 20) {
    println("---------- tables -------- ")
    nodes.foreach { case (ep, node) =>
      
      val info = Await.result(node ? DhtNode.Describe, timeoutDuration).asInstanceOf[DhtNode.NodeInfo]
      
      println("dht table for: " + info.self.id + "@" + info.self.address)
      info.nodes.foreach { entry => println("\t" + entry)}
    }    

    Thread.sleep(30 * 1000)
    
    println("---------- FIND NODE -------- ")
    
    nodes.foreach { case (_, n) =>
      n ! Controller.FindNode(Integer160.random)
      Thread.sleep(2 * 1000)
    }
  }

  nodes.foreach { case (_, n) => n ! DhtNode.Stop }

  Thread.sleep(3 * 1000)
  
  system.shutdown()
  
  Thread.sleep(1 * 1000)
}
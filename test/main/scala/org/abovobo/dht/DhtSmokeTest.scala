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



object DhtSmokeTest extends App {
	val systemConfig = ConfigFactory.parseMap(Map(
	    "akka.log-dead-letters" -> "true", 
	    "akka.actor.debug.lifecycle" -> true,
	    "akka.loglevel" -> "debug",
	    
		"akka.actor.debug.receive" -> true,
		"akka.actor.debug.unhandled" -> true))
		
		
	case class NodeSystem(endpoint: InetSocketAddress, table: ActorRef, agent: ActorRef, controller: ActorRef, system: ActorSystem, storage: H2Storage) extends Disposable {
	  def dispose() {
        system.shutdown()
        system.awaitTermination()
        storage.close()	    
	  }
	}
  
	def localEndpoint(ordinal: Int) = new InetSocketAddress(InetAddress.getLocalHost, 20000 + ordinal)
		
	def createNode(ordinal: Int, routers: List[InetSocketAddress] = List()): NodeSystem = {
	    val h2 = H2Storage.open("~/db/dht-" + ordinal, true) 

		val storage: Storage = h2
		val reader: Reader = h2
		val writer: Writer = h2
			
		val system = ActorSystem("TestSystem-" + ordinal, systemConfig)

    val controller = system.actorOf(Controller.props(routers, reader, writer), "controller")
    val agent = system.actorOf(Agent.props(localEndpoint(ordinal), 10 seconds, controller), "agent")		
		val table = system.actorOf(Table.props(reader, writer, controller), "table")		
	
		NodeSystem(localEndpoint(ordinal), table, agent, controller, system, h2)
	}
	
	var systems = List(createNode(1))
	//val routerEndpoints = List(localEndpoint(1))

	Thread sleep 5000

	
	for (i <- 2 to 100) {
	  Thread sleep 500
	  systems ::= createNode(i, List(localEndpoint(1)))
	}
	
	Thread sleep 35000
	
	// 
	println("printing nodes")
	
	systems.sortBy { _.endpoint.getPort() }.foreach { ns =>
	  print("node " + ns.storage.id + "@" + ns.endpoint + " => " )
	  println(ns.storage.nodes.map { n => n.id.toString.substring(0, 4) + "...@" + n.address }.mkString(","))
	}
	
	systems foreach { _.dispose }
}
package org.abovobo.dht

import org.abovobo.integer.Integer160
import java.net.InetSocketAddress
import akka.actor.{ActorLogging, Actor}

/**
 * Created by dmitryzhuk on 20.01.14.
 */
class Controller extends Actor with ActorLogging {

  import Controller._

  override def receive = {
    case Ping(address) =>
  }

  /// Initializes sibling `agent` actor reference
  private lazy val agent = this.context.actorSelection("../agent")
}

object Controller {

  sealed trait Message

  sealed trait Command extends Message

  case class Ping(address: InetSocketAddress) extends Command
  case class FindNode(id: Integer160) extends Command
  case class GetPeers(hash: Integer160) extends Command

}

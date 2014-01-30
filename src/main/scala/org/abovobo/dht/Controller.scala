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
    case Fail(query) =>
  }

  /// Initializes sibling `agent` actor reference
  private lazy val agent = this.context.actorSelection("../agent")
}

object Controller {

  sealed trait Command

  case class Fail(query: Query) extends Command
  case class Received(message: Message) extends Command

  case class FindNode(target: Integer160) extends Command
  case class Ping(address: InetSocketAddress) extends Command
}

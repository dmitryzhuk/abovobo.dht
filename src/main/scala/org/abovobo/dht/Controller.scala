package org.abovobo.dht

import org.abovobo.integer.Integer160
import java.net.InetSocketAddress

/**
 * Created by dmitryzhuk on 20.01.14.
 */
class Controller {

}

object Controller {

  sealed trait Command

  case class Ping(address: InetSocketAddress) extends Command
  case class FindNode(id: Integer160) extends Command
  case class GetPeers(hash: Integer160) extends Command
}

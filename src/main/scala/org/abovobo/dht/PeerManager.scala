/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht

import akka.actor.{ActorLogging, Actor}
import org.abovobo.integer.Integer160
import java.net.InetSocketAddress

/**
 * Created by dmitryzhuk on 12.02.14.
 */
class PeerManager extends Actor with ActorLogging  {

  import PeerManager._

  override def receive = {
    case Store(infohash, peer) =>
  }
}

object PeerManager {
  sealed trait Command
  case class Store(infohash: Integer160, peer: Peer)
}

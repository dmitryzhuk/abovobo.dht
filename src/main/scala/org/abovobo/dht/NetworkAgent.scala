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

import akka.io.{Tcp, Udp, IO}
import akka.actor.{ActorRef, Actor}
import java.net.InetSocketAddress

/**
 * Created by dmitryzhuk on 09.01.14.
 */
class NetworkAgent extends Actor {

  import this.context.system

  override def preStart() = {
    /// Initialize UDP bind procedure
    IO(Udp) ! Udp.Bind(self, new InetSocketAddress(0))
  }

  override def postStop() = {
    IO(Udp) ! Udp.Unbind
  }

  override def receive = {
    case Udp.Bound(local) => this.context.become(this.ready(sender))
    case Tcp.Bound(local) => this.context.become(this.ready(sender))
  }

  def ready(socket: ActorRef): Receive = {
    case Udp.Received(data, remote) => // TODO Implement message receiving
    case Udp.Unbind => socket ! Udp.Unbind
    case Udp.Unbound => this.context.unbecome()
  }

}

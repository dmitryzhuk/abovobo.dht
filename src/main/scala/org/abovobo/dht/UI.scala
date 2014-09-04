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

import java.net.InetSocketAddress

import akka.pattern.ask
import akka.actor.ActorLogging
import akka.io.{Tcp, IO}
import akka.util.Timeout
import spray.can.Http
import spray.routing.{HttpServiceActor, Route}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * This actor sets up HTTP listener and provides web UI for nodes.
 *
 * @param endpoint An endpoint to listen at.
 */
class UI(val endpoint: InetSocketAddress) extends HttpServiceActor with ActorLogging {

  /** Defines HTTP handling */
  val route: Route = {
    get {
      pathSingleSlash {
        complete("Hello, world!")
      } ~
      path("test") {
        complete("Test page")
      }
    }
  }

  /** @inheritdoc */
  override def preStart() = {
    this.log.debug("UI#preStart")
    implicit val timeout: Timeout = 1.second
    import this.context.system
    import this.context.dispatcher
    IO(Http).ask(Http.Bind(this.self, this.endpoint, 100, Nil, None)).flatMap {
      case b: Http.Bound =>
        this.log.info("UI is bound")
        Future.successful(b)
      case Tcp.CommandFailed(b: Http.Bind) =>
        this.log.error("Failed to bind UI")
        Future.failed(throw new RuntimeException(
          "Binding failed. Set DEBUG-level logging for `akka.io.TcpListener` to log the cause."))
    }
  }

  /** @inheritdoc */
  override def receive = this.runRoute(this.route)

}

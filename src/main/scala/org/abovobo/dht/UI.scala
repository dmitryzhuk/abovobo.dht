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

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef}
import akka.io.{IO, Tcp}
import akka.pattern.ask
import akka.util.Timeout
import org.abovobo.dht.json.NodesJsonProtocol._
import spray.can.Http
import spray.http.MediaTypes._
import spray.http.{HttpRequest, StatusCodes, Timedout}
import spray.json._
import spray.routing._
import spray.util.LoggingContext

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
 * This actor sets up HTTP listener and provides web UI for nodes.
 *
 * @param endpoint An endpoint to listen at.
 * @param nodes    A collection of nodes which should be accessible via this UI.
 * @param stopper  A reference to actor which will handle stop requests from UI.
 */
class UI(val endpoint: InetSocketAddress,
         val nodes: IndexedSeq[Node],
         val stopper: ActorRef)
  extends HttpServiceActor with ActorLogging {

  /** Extract router nodes from normal nodes */
  val partitions = this.nodes.partition(_.routers.isEmpty)

  /** Defines HTTP handling */
  val route: Route = {
    get {
      //
      // Accessing the root causes redirection to index.html
      pathSingleSlash {
        redirect("static/index.html", StatusCodes.MovedPermanently)
      } ~
      path("stop") {
        complete {
          this.stopper ! UI.Shutdown
          "Server shut down"
        }
      } ~
      //
      // This clause handles all static content
      pathPrefix("static") {
        path(Segments) { path =>
          this.log.debug("Accessing static file: " + path.reduceLeft(_ + "/" + _))
          val ct = path.last.split("\\.").last match {
            case "html" | "htm" => `text/html`
            case "css" => `text/css`
            case "js" => `application/javascript`
            case _ => `text/plain`
          }
          getFromResource(path.reduceLeft(_ + "/" + _), ct)
        }
      } ~
      //
      // Lists all nodes available at this location
      path("list" / IntNumber / IntNumber) { (offset, count) =>
        complete {
          JsObject(
            ("routers", this.partitions._1.toJson),
            ("nodes", this.partitions._2.toJson)
          ).compactPrint
        }
      }
    }
  }

  /** @inheritdoc */
  override def preStart() = {
    this.log.debug("UI#preStart")

    import context.{dispatcher, system}

    implicit val timeout: Timeout = 1.second

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
  override def runRoute(route: Route)(implicit eh: ExceptionHandler, rh: RejectionHandler, ac: ActorContext,
                             rs: RoutingSettings, log: LoggingContext): Actor.Receive = {
    val sealedExceptionHandler = eh orElse ExceptionHandler.default
    val sealedRoute = sealRoute(route)(sealedExceptionHandler, rh)
    def runSealedRoute(ctx: RequestContext): Unit =
      try sealedRoute(ctx)
      catch {
        case NonFatal(e) ⇒
          val errorRoute = sealedExceptionHandler(e)
          errorRoute(ctx)
      }

    {
      case request: HttpRequest ⇒
        val ctx = RequestContext(request, ac.sender(), request.uri.path).withDefaultSender(ac.self)
        runSealedRoute(ctx)

      case ctx: RequestContext ⇒ runSealedRoute(ctx)

      case Tcp.Connected(_, _) ⇒
        // by default we register ourselves as the handler for a new connection
        ac.sender() ! Tcp.Register(ac.self)

      case x: Tcp.ConnectionClosed        ⇒ onConnectionClosed(x)

      case Timedout(request: HttpRequest) ⇒ runRoute(timeoutRoute)(eh, rh, ac, rs, log)(request)

      case Requester.PeerAnnounced =>
        this.log.debug("Peer announced!")
    }
  }

  /** @inheritdoc */
  override def receive = this.runRoute(this.route)

}

/** Accompanying object */
object UI {

  /** Base trait for all events fired by UI */
  sealed trait Event

  /** Event indicating that system shutdown has been requested */
  case object Shutdown extends Event
}
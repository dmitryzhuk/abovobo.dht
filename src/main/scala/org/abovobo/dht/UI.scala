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
import org.abovobo.dht.json.NodeJsonProtocol._
import org.abovobo.integer.Integer160
import spray.can.Http
import spray.http.HttpHeaders._
import spray.http.CacheDirectives._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, StatusCodes, Timedout}
import spray.json._
import spray.routing._
import spray.util.LoggingContext
import akka.actor.ActorDSL._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
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

  import this.context.dispatcher

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
          this.context.system.scheduler.scheduleOnce(1.second, this.stopper, UI.Shutdown)
          ""
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
          respondWithHeader(`Cache-Control`(`no-cache`, `no-store`)) {
            getFromResource(path.reduceLeft(_ + "/" + _), ct)
          }
        }
      } ~
      //
      // Groups calls to nodes collection
      pathPrefix("nodes") {
        // Lists all nodes available at this location
        path("list" / IntNumber / IntNumber) { (offset, count) =>
          complete {
            val routers = this.partitions._1.drop(offset).take(count)
            val nodes = this.partitions._2
              .drop(if (offset > this.partitions._1.size) offset - this.partitions._1.size else 0)
              .take(if (count > routers.size) count - routers.size else 0)
            JsObject(("routers", routers.toJson), ("nodes", nodes.toJson)).compactPrint
          }
        } ~
        // number of available nodes
        path("count") {
          complete(JsNumber(this.nodes.size).compactPrint)
        }
      } ~
      //
      // Produces complete node description
      pathPrefix("node") {
        path(LongNumber) { lid =>
          complete {
            this.nodes
              .find(_.id == lid)
              .map(_.toJson.compactPrint)
              .getOrElse[String](JsNull.compactPrint)
          }
        } ~
        path("announce" / Segment / LongNumber) { (hash, lid) =>
          implicit val timeout: akka.util.Timeout = 30.seconds
          complete {
            import spray.json.DefaultJsonProtocol._
            this.nodes.find(_.id == lid).map { node =>
              actor(new Act {
                var initiator: ActorRef = null
                var total: Int = 0
                var results = new ListBuffer[Requester.Result]()
                become {
                  case msg @ Requester.GetPeers(infohash) =>
                    node.requester ! msg
                    this.initiator = this.sender()
                  case Requester.Found(target, infos, peers, tokens) =>
                    UI.this.log.debug("Found {} nodes for {}", infos.size, target)
                    this.total = infos.size
                    infos.foreach { info =>
                      node.requester ! Requester.AnnouncePeer(
                        info,
                        tokens(info.id),
                        new Integer160(hash),
                        node.endpoint.getPort,
                        implied = false)
                    }
                  case msg @ Requester.NotFound =>
                    UI.this.log.error("Not found for {}", hash)
                    this.initiator ! msg
                  case msg @ Requester.PeerAnnounced(target, info) =>
                    UI.this.log.debug("Peer announced for {}", hash)
                    this.results += msg
                    if (this.results.size == this.total) {
                      this.initiator ! this.results.toArray
                    }
                  case msg @ Requester.Failed(query) =>
                    this.results += msg
                    if (this.results.size == this.total) {
                      this.initiator ! this.results.toArray
                    }
                }
              }) ? Requester.GetPeers(new Integer160(hash))
            }.map(Await.result(_, 30.seconds)).map {
                case results: Array[Requester.Result] => results
                case _ => Array.empty[Requester.Result]
            }.getOrElse(Array.empty[Requester.Result]).map {
              case Requester.Failed(query) =>
                JsObject("status" -> "failed".toJson)
              case Requester.PeerAnnounced(target, remote) =>
                JsObject("status" -> "announced".toJson, "remote" -> remote.id.toString.toJson)
              case _ =>
                JsObject("status" -> "unknown".toJson)
            }.toJson.compactPrint
          }
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
    val sealedRoute = this.sealRoute(route)(sealedExceptionHandler, rh)
    def runSealedRoute(ctx: RequestContext): Unit =
      try sealedRoute(ctx)
      catch {
        case NonFatal(e) ⇒
          val errorRoute = sealedExceptionHandler(e)
          errorRoute(ctx)
      }

    {
      case request: HttpRequest =>
        val ctx = RequestContext(request, ac.sender(), request.uri.path).withDefaultSender(ac.self)
        runSealedRoute(ctx)

      case ctx: RequestContext => runSealedRoute(ctx)

      // by default we register ourselves as the handler for a new connection
      case Tcp.Connected(_, _) => ac.sender() ! Tcp.Register(ac.self)

      case x: Tcp.ConnectionClosed => this.onConnectionClosed(x)

      case Timedout(request: HttpRequest) => this.runRoute(timeoutRoute)(eh, rh, ac, rs, log)(request)

      case Requester.PeerAnnounced =>
        this.log.debug("Peer announced!")

      case Requester.Found(target, infos, peers, tokens) =>
        this.log.debug("====")
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
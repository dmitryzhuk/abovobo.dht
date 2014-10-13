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
import org.abovobo.dht.json.RequesterResultJsonProtocol._
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
  implicit val timeout: Timeout = 30.seconds

  /** Defines HTTP handling */
  val route: Route = {
    get {
      //
      // Accessing the root causes redirection to index.html
      pathSingleSlash {
        redirect("static/index.html", StatusCodes.MovedPermanently)
      } ~
      //
      // Gracefully stops this application
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
      // Operations performed on the particular node
      pathPrefix("node") {
        // Returns complete description of number with given local ID
        path(LongNumber) { lid =>
          complete {
            this.nodes
              .find(_.id == lid)
              .map(_.toJson.compactPrint)
              .getOrElse[String](JsNull.compactPrint)
          }
        } ~
        // Announces given hashinfo into DHT
        path("announce" / Segment / LongNumber) { (hash, lid) =>
          complete {
            this.announce(new Integer160(hash), this.node(lid)).toJson.prettyPrint
          }
        } ~
        path("find" / Segment / LongNumber) { (hash, lid) =>
          complete {
            this.find(new Integer160(hash), this.node(lid)).toJson.prettyPrint
          }
        }
      }
    }
  }

  /** @inheritdoc */
  override def preStart() = {
    this.log.debug("UI#preStart")

    import context.{dispatcher, system}

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
    }
  }

  /** @inheritdoc */
  override def receive = this.runRoute(this.route)

  /**
   * Returns particular node with given local ID. Fails with exception if not found.
   *
   * @param lid Local ID of the node to return.
   * @return A node with given local ID.
   */
  private def node(lid: Long) = this.nodes.find(_.id == lid).get

  /**
   * Performs recursive node lookup procedure.
   *
   * @param target   An infohash value which is target for lookup procedure.
   * @param node     A node from which the lookup must be performed.
   * @return         A result of this request.
   */
  private def find(target: Integer160, node: Node): Requester.Found =
    Await.result(node.requester.ask(Requester.GetPeers(target)), this.timeout.duration).asInstanceOf[Requester.Found]

  /**
   * Announces given infohash from given node to DHT.
   *
   * @param infohash An infohash to announce.
   * @param node     A node from which announce must be performed.
   * @return         An array of announce results.
   */
  private def announce(infohash: Integer160, node: Node): Array[Requester.Result] =
    Await.result(actor(new Act {
      var total: Int = 0
      var results = new ListBuffer[Requester.Result]()
      var initiator = ActorRef.noSender
      become {
        case msg@Requester.GetPeers(_) =>
          node.requester ! msg
          this.initiator = this.sender()
        case Requester.Found(target, infos, peers, tokens, rounds) =>
          this.total = infos.size
          infos.foreach { info =>
            node.requester ! Requester.AnnouncePeer(
              info, tokens(info.id), infohash, node.endpoint.getPort, implied = false)
          }
        case msg@Requester.NotFound =>
          this.initiator ! Array(msg)
        case msg@Requester.PeerAnnounced(target, info) =>
          this.results += msg
          if (this.results.size == this.total) {
            this.initiator ! this.results.toArray
          }
        case msg@Requester.Failed(query, remote) =>
          this.results += msg
          if (this.results.size == this.total) {
            this.initiator ! this.results.toArray
          }
      }
    }).ask(Requester.GetPeers(infohash)), this.timeout.duration).asInstanceOf[Array[Requester.Result]]
}

/** Accompanying object */
object UI {

  /** Base trait for all events fired by UI */
  sealed trait Event

  /** Event indicating that system shutdown has been requested */
  case object Shutdown extends Event
}
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

import org.abovobo.integer.Integer160
import java.net.InetSocketAddress
import akka.actor.{ActorLogging, Actor}
import scala.collection.mutable
import org.abovobo.dht.persistence.{Writer, Reader}
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * This Actor actually controls processing of the messages implementing recursive DHT algorithms.
 *
 * @constructor     Creates new instance of controller with provided parameters.
 *
 * @param K         Max number of entries to send back to querier.
 * @param alpha     Number of concurrent lookup threads.
 * @param period    A period of rotating the token.
 * @param lifetime  A lifetime of the peer info.
 * @param reader    Instance of [[org.abovobo.dht.persistence.Reader]] used to access persisted data.
 * @param writer    Instance of [[org.abovobo.dht.persistence.Writer]] to update persisted DHT state.
 *
 * @author Dmitry Zhuk
 */
class Controller(val K: Int,
                 val alpha: Int,
                 val period: FiniteDuration,
                 val lifetime: FiniteDuration,
                 val reader: Reader,
                 val writer: Writer)
  extends Actor with ActorLogging {

  import Controller._

  /**
   * @inheritdoc
   */
  override def preStart() = {
    // reference lazy val just to initialize it
    require(!this.cleanupTokensTask.isCancelled)
    require(!this.cleanupPeersTask.isCancelled)
  }

  /**
   * @inheritdoc
   */
  override def postStop() = {
    this.cleanupTokensTask.cancel()
    this.cleanupPeersTask.cancel()
    this.tp.stop
  }

  /**
   * @inheritdoc
   *
   * Implements handling of [[org.abovobo.dht.Controller]]-specific events and commands.
   */
  override def receive = {
    // -- HANDLE COMMANDS
    // -- ---------------
    case Ping(address: InetSocketAddress) =>
      // Simply send Query.Ping to remote peer
      val query = new Query.Ping(this.factory.next(), this.reader.id().get)
      this.transactions.put(query.tid, query -> address)
      this.agent ! Agent.Send(query, address)

    case FindNode(target: Integer160) =>
      // --
      this.find(target)

    // -- HANDLE EVENTS
    // -- -------------
    case Failed(query) =>
      // when our query has failed just remove corresponding transaction
      // and properly notify routing table about this failure
      this.transactions.get(query.tid) match {
        case Some(pair) =>
          this.transactions.remove(query.tid)
          this.table ! Table.Received(new Node(query.id, pair._2), Message.Kind.Fail)
        case None => // Error: invalid transaction
          this.log.error("Failed event with invalid transaction: " + query)
      }

    case Received(message, remote) =>
      // received message handled differently depending on message type
      message match {
        case response: Response =>
          // if response has been received
          // close transaction, notify routing table and then
          // delegate execution to private `process` method
          this.transactions.get(response.tid) match {
            case Some(pair) =>
              this.transactions.remove(response.tid)
              this.table ! Table.Received(new Node(pair._1.id, pair._2), Message.Kind.Response)
              this.process(response, pair._1, pair._2)
            case None => // Error: invalid transaction
              this.log.error("Response message with invalid transaction: " + response)
          }

        case query: Query =>
          // if query has been received
          // notify routing table and then delegate execution to private `respond` method
          this.table ! Table.Received(new Node(query.id, remote), Message.Kind.Query)
          this.respond(query, remote)

        case error: Error =>
          // if error message has been received
          // close transaction and notify routing table
          this.transactions.get(error.tid) match {
            case Some(pair) =>
              this.transactions.remove(error.tid)
              this.table ! Table.Received(new Node(pair._1.id, pair._2), Message.Kind.Error)
            case None => // Error: invalid transaction
              this.log.error("Error message with invalid transaction: " + error)
          }
      }
  }

  private def find(target: Integer160) = {
    val nodes = this.klosest(target)
  }

  private def process(response: Response, query: Query, remote: InetSocketAddress) = {
    query match {
      case ping: Query.Ping =>
        // nothing to do
    }
  }

  /**
   * Responds to a query sending appropriate response message back to remote peer.
   *
   * @param query   A query to respond to.
   * @param remote  An address of the remote peer to send response to.
   */
  private def respond(query: Query, remote: InetSocketAddress) = {
    // read this node SHA-1 identifier
    val id = this.reader.id().get

    query match {

      case ping: Query.Ping =>
        // simply send response `ping` message
        this.agent ! Agent.Send(new Response.Ping(query.tid, id), remote)

      case fn: Query.FindNode =>
        // get `K` closest nodes from own routing table
        val nodes = this.klosest(fn.target)
        // now respond with proper message
        this.agent ! Agent.Send(new Response.FindNode(fn.tid, id, nodes), remote)

      case gp: Query.GetPeers =>
        // get current token to return to querier
        val token = this.tp.get
        // look for peers for the given infohash in the storage
        val peers = this.reader.peers(gp.infohash)
        if (!peers.isEmpty) {
          // if there are peers found send them with response
          this.agent ! Agent.Send(new Response.GetPeersWithValues(gp.tid, id, token, peers.toSeq), remote)
        } else {
          // otherwise send closest K nodes
          val nodes = this.klosest(gp.infohash)
          this.agent ! Agent.Send(new Response.GetPeersWithNodes(gp.tid, id, token, nodes), remote)
        }
        // remember that we sent the token to this remote peer
        this.tokens.get(remote).getOrElse(Nil) match {
          case Nil => this.tokens.put(remote, List(token))
          case l => this.tokens.put(remote, token :: l)
        }

      case ap: Query.AnnouncePeer =>
        this.tokens.get(remote).getOrElse(Nil) match {
          case Nil => // error
            this.agent ! Agent.Send(
              new Error(
                ap.tid,
                Error.ERROR_CODE_PROTOCOL,
                "Address is missing in token distribution list"),
              remote)
          case l: List[Token] =>
            l.find(_.sameElements(ap.token)) match {
              case Some(value) =>
                if (this.tp.valid(value)) {
                  this.writer.transaction {
                    this.writer.announce(
                      ap.infohash,
                      new Peer(remote.getAddress, if (ap.implied) remote.getPort else ap.port))
                  }
                } else {
                  this.agent ! Agent.Send(
                    new Error(
                      ap.tid,
                      Error.ERROR_CODE_GENERIC,
                      "Token has expired"),
                    remote)
                }
              case None => // error
                this.agent ! Agent.Send(
                  new Error(
                    ap.tid,
                    Error.ERROR_CODE_PROTOCOL,
                    "Given token was not distributed to this peer"),
                  remote)
            }
        }
        this.agent ! Agent.Send(new Response.AnnouncePeer(query.tid, id), remote)
    }

  }

  /**
   * This method is executed periodically to cleanup `tokens` collection
   * thus removing those token-peer association which has expired.
   */
  private def cleanupTokens() = {
    val keys = this.tokens.keySet
    keys foreach { key =>
      this.tokens.get(key).getOrElse(Nil).filter(this.tp.valid) match {
        case Nil => this.tokens.remove(key)
        case l => this.tokens.put(key, l)
      }
    }
  }

  /**
   * Retrieves K closest nodes to target from routing table.
   *
   * @param target A target ID to find closest nodes to.
   * @return K closest nodes to target from routing table.
   */
  private def klosest(target: Integer160) = {
    val nodes = this.reader.nodes()
      .map(node => node -> (node.id ^ target)).toSeq
      .sortWith(_._2 < _._2)
      .take(this.K)
      .map(_._1)
    nodes
  }

  /** This method is executed periodically to remote expired infohash-peer associations */
  private def cleanupPeers() = this.writer.cleanup(this.lifetime)

  /// Initializes sibling `agent` actor reference
  private lazy val agent = this.context.actorSelection("../agent")

  /// Initializes sibling `table` actor reference
  private lazy val table = this.context.actorSelection("../table")

  /// Initializes sibling `accumulator` actor reference
  private lazy val accumulator = this.context.actorSelection("../accumulator")

  private lazy val cleanupPeersTask =
    this.context.system.scheduler.schedule(Duration.Zero, this.lifetime)(this.cleanupPeers())(this.context.dispatcher)

  /// Initializes token provider
  private lazy val tp = new TokenProvider(this.period, this.context.system.scheduler, this.context.dispatcher)

  /// Periodic task which cleans up `tokens` collection
  private lazy val cleanupTokensTask =
    this.context.system.scheduler.schedule(Duration.Zero, this.period * 2)(this.cleanupTokens())(this.context.dispatcher)

  /// Collection of remote peers which received tokens
  private val tokens = new mutable.HashMap[InetSocketAddress, List[Token]]

  /// Instantiate transaction ID factory
  private val factory = new TIDFactory

  /// Instance of tracked queries
  private val transactions = new mutable.HashMap[TID, (Query, InetSocketAddress)]
}

/** Accompanying object */
object Controller {

  /** Base trait for all events fired by other actors */
  sealed trait Event

  /**
   * This event indicates that there was a query sent to remote peer, but remote peer failed to respond
   * in timely manner.
   *
   * @param query A query which has been sent initially.
   */
  case class Failed(query: Query) extends Event

  /**
   * This event indicates that there was a message received from the remote peer.
   *
   * @param message A message received.
   * @param remote  An address of the remote peer which sent us that message.
   */
  case class Received(message: Message, remote: InetSocketAddress) extends Event

  /** Base trait for all commands supported by this actor */
  sealed trait Command

  /**
   * This command instructs controller to send `ping` message to the given address.
   *
   * @param address An address to send `ping` message to.
   */
  case class Ping(address: InetSocketAddress) extends Command

  case class FindNode(target: Integer160) extends Command
}

/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.json

import org.abovobo.dht.Node
import org.abovobo.dht.persistence.Storage
import org.abovobo.integer.Integer160
import org.abovobo.jdbc.Closer._
import spray.json._
import org.abovobo.dht.json.KnownNodeInfoProtocol._

/** Defines JSON protocol for the collection of nodes */
object NodeJsonProtocol extends DefaultJsonProtocol {

  /** Implicitly defines JSON format for indexed sequence of Node objects */
  implicit object NodeJsonFormat extends RootJsonFormat[Node] {

    /** @inheritdoc */
    override def write(node: Node) =
      using(node.sf(node.id)) { storage =>
        JsObject(
          new JsField("uid", JsString(storage.id().getOrElse(Integer160.maxval).toString)),
          new JsField("lid", JsNumber(node.id)),
          new JsField("address", JsString(node.endpoint.getAddress.toString)),
          new JsField("port", JsNumber(node.endpoint.getPort)),
          new JsField("table", this.table(storage, node)),
          new JsField("peers", this.peers(storage, node))
        )
      }

    /** We will never deserialize collections of Node objects from JSON */
    override def read(value: JsValue) = throw new NotImplementedError()

    /**
     * Reads whole routing table and converts it into JSON representation.
     *
     * @param storage a storage to get data from.
     * @param node    a node to get data of.
     * @return  JSON representation of routing table.
     */
    private def table(storage: Storage, node: Node) = {
      storage.buckets().map { bucket =>
        JsObject(
          "start" -> bucket.start.toString.toJson,
          "end" -> bucket.end.toString.toJson,
          "seen" -> bucket.seen.getTime.toJson,
          "nodes" -> storage.nodes(bucket).map(_.toJson).toSeq.toJson
        )
      }.toList.toJson
    }

    /**
     * Reads collection of peers and coverts it into JSON representation.
     *
     * @param storage a storage to get data from.
     * @param node    a node to get data of.
     * @return JSON representation of collection of stored peers.
     */
    private def peers(storage: Storage, node: Node) = {
      storage.peers().map { peer =>
        JsObject(
          "address" -> peer.address.getAddress.toString.toJson,
          "port" -> peer.address.getPort.toJson,
          "infohash" -> peer.infohash.toString.toJson,
          "announced" -> peer.announced.getTime.toJson
        )
      }.toList.toJson
    }
  }

}

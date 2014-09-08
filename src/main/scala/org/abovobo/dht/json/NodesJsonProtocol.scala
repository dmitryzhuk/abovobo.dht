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
import org.abovobo.integer.Integer160
import org.abovobo.jdbc.Closer._
import spray.json._

/** Defines JSON protocol for the collection of nodes */
object NodesJsonProtocol extends DefaultJsonProtocol {

  /** Implicitly defines JSON format for indexed sequence of Node objects */
  implicit object NodesJsonFormat extends RootJsonFormat[IndexedSeq[Node]] {

    /** @inheritdoc */
    override def write(nodes: IndexedSeq[Node]) = {
      JsArray(
        nodes.map { node =>
          using(node.sf()) { storage =>
            JsObject(
              new JsField("uid", JsString(storage.id().getOrElse(Integer160.maxval).toString)),
              new JsField("lid", JsNumber(node.id)),
              new JsField("address", JsString(node.endpoint.getAddress.toString)),
              new JsField("port", JsNumber(node.endpoint.getPort)),
              new JsField("buckets", JsNumber(storage.buckets().size)),
              new JsField("nodes", JsNumber(storage.nodes().size)),
              new JsField("peers", JsNumber(storage.peers().size))
            )
          }
        }.toList
      )
    }

    /** We will never deserialize collections of Node objects from JSON */
    override def read(value: JsValue) = throw new NotImplementedError()
  }

}

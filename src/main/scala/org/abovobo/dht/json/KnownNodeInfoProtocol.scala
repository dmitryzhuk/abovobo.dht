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

import org.abovobo.dht.{KnownNodeInfo, Node}
import org.abovobo.dht.persistence.Storage
import org.abovobo.integer.Integer160
import org.abovobo.jdbc.Closer._
import spray.json._

/** Defines JSON protocol for the collection of nodes */
object KnownNodeInfoProtocol extends DefaultJsonProtocol {

  /** Implicitly defines JSON format for indexed sequence of Node objects */
  implicit object KnownNodeInfoJsonFormat extends RootJsonFormat[KnownNodeInfo] {

    /** @inheritdoc */
    override def write(node: KnownNodeInfo) =
      JsObject(
        "uid" -> node.id.toString.toJson,
        "address" -> node.address.getAddress.toString.toJson,
        "port" -> node.address.getPort.toJson,
        "failcount" -> node.failcount.toJson,
        "queried" -> node.queried.map(_.getTime.toJson).getOrElse(JsNull),
        "replied" -> node.replied.map(_.getTime.toJson).getOrElse(JsNull)
      )

    /** We will never deserialize collections of Node objects from JSON */
    override def read(value: JsValue) = throw new NotImplementedError()
  }

}

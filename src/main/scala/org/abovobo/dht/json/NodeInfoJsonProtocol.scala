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

import org.abovobo.dht.{NodeInfo, KnownNodeInfo}
import spray.json._

/** Defines JSON protocol for the collection of nodes */
object NodeInfoJsonProtocol extends DefaultJsonProtocol {

  /** Implicitly defines JSON format for indexed sequence of Node objects */
  implicit object NodeInfoJsonFormat extends RootJsonFormat[NodeInfo] {

    /** @inheritdoc */
    override def write(node: NodeInfo) =
      JsObject(
        "uid" -> node.id.toString.toJson,
        "address" -> node.address.getAddress.toString.toJson,
        "port" -> node.address.getPort.toJson
      )

    /** We will never deserialize collections of Node objects from JSON */
    override def read(value: JsValue) = throw new NotImplementedError()
  }

  implicit object NodeInfosJsonFormat extends RootJsonFormat[Traversable[NodeInfo]] {

    /** @inheritdoc */
    override def write(nodes: Traversable[NodeInfo]) = JsArray(nodes.map(_.toJson).toList)

    /** We will never deserialize collections of Node objects from JSON */
    override def read(value: JsValue) = throw new NotImplementedError()
  }

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

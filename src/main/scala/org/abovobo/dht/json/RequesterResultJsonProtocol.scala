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

import org.abovobo.dht.Requester
import org.abovobo.dht.json.MessageJsonProtocol._
import org.abovobo.dht.json.NodeInfoJsonProtocol._
import spray.json.{JsObject, JsValue, RootJsonFormat, DefaultJsonProtocol}
import spray.json._

/**
 * This protocol handles conversion of any [[Requester.Result]] type into JSON.
 */
object RequesterResultJsonProtocol extends DefaultJsonProtocol {

  implicit object RequesterResultJsonFormat extends RootJsonFormat[Requester.Result] {

    /** @inheritdoc */
    override def write(result: Requester.Result) = result match {
      case Requester.Failed(q, remote) =>
        JsObject("result" -> "fail".toJson, "query" -> q.toJson, "remote" -> remote.toJson)
      case Requester.NotFound(target, rounds) =>
        JsObject("result" -> "fail".toJson, "target" -> target.toString.toJson, "rounds" -> rounds.toJson)
      case Requester.Found(target, nodes, peers, tokens, rounds) =>
        JsObject(
          "result" -> "success".toJson,
          "target" -> target.toString.toJson,
          "nodes" -> nodes.toJson,
          "peers" -> JsArray(peers.map(_.toString.toJson).toList),
          "rounds" -> rounds.toJson
        )
      case Requester.Pinged(q, remote) =>
        JsObject("result" -> "success".toJson, "query" -> q.toJson, "remote" -> remote.toJson)
      case Requester.PeerAnnounced(q, remote) =>
        JsObject("result" -> "success".toJson, "query" -> q.toJson, "remote" -> remote.toJson)
    }

    /** We will never deserialize collections of Node objects from JSON */
    override def read(value: JsValue) = throw new NotImplementedError()
  }

  implicit object RequesterResultsJsonFormat extends RootJsonFormat[Array[Requester.Result]] {

    /** @inheritdoc */
    override def write(results: Array[Requester.Result]) = JsArray(results.map(_.toJson).toList)

    /** We will never deserialize collections of Node objects from JSON */
    override def read(value: JsValue) = throw new NotImplementedError()
  }
}

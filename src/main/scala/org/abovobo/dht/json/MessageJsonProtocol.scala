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

import org.abovobo.dht.message.Query
import spray.json.{JsValue, JsObject, RootJsonFormat, DefaultJsonProtocol}

/**
 * This protocol handles conversion of DHT messages into JSON.
 */
object MessageJsonProtocol extends DefaultJsonProtocol {

  implicit object QueryJsonFormat extends RootJsonFormat[Query] {

    /** @inheritdoc */
    override def write(query: Query) = {
      //
      JsObject()
    }

    /** We will never deserialize collections of Node objects from JSON */
    override def read(value: JsValue) = throw new NotImplementedError()

  }

  implicit object AnnouncePeerJsonFormat extends RootJsonFormat[Query.AnnouncePeer] {

    /** @inheritdoc */
    override def write(query: Query.AnnouncePeer) = {
      //
      JsObject()
    }

    /** We will never deserialize collections of Node objects from JSON */
    override def read(value: JsValue) = throw new NotImplementedError()

  }

  implicit object PingJsonFormat extends RootJsonFormat[Query.Ping] {

    /** @inheritdoc */
    override def write(query: Query.Ping) = {
      //
      JsObject()
    }

    /** We will never deserialize collections of Node objects from JSON */
    override def read(value: JsValue) = throw new NotImplementedError()

  }

}

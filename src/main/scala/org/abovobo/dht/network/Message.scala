package org.abovobo.dht.network

/**
 * Created by dmitryzhuk on 06.01.14.
 */
object Message {

  /**
   * This enumeration defines possible kinds of network message:
   *
   * Query means that remote node has sent us a query message,
   * Reply means that remote node has replied with correct message to our query,
   * Error means that remote node has replied with error message to our query,
   * Fail  means that remote node failed to reply.
   */
  object Kind extends Enumeration {
    type Kind = Value
    val Query, Reply, Error, Fail = Value
  }

}

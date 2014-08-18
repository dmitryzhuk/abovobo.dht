/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.persistence.h2

/**
 * Object defining actual H2 SQL queries for [[org.abovobo.dht.persistence.Writer]] trait.
 */
object Writer {
  val Q_SET_SELF_ID_STMT = "insert into self(id) values(?)"
  val Q_DROP_SELF_ID_STMT = "delete from self"
  val Q_INSERT_NODE_STMT = "insert into node(id, address, replied, queried) values(?, ?, ?, ?)"
  val Q_UPDATE_NODE_STMT = "update node set address=?, replied=?, queried=?, failcount=? where id=?"
  val Q_DELETE_NODE_STMT = "delete from node where id=?"
  val Q_INSERT_BUCKET_STMT = "insert into bucket(id, seen) values(?, now())"
  val Q_TOUCH_BUCKET_STMT = "update bucket set seen=now() where id=?"
  val Q_DROP_ALL_BUCKETS_STMT = "delete from bucket"
  val Q_ANNOUNCE_PEER_STMT = "merge into peer(infohash, address, announced) values(?, ?, now())"
  val Q_CLEANUP_PEERS_STMT = "delete from peer where dateadd('SECOND', ?, announced) < now()"
}

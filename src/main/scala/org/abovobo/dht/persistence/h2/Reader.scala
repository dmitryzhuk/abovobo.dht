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
 * Object defining actual H2 SQL queries for [[org.abovobo.dht.persistence.Reader]] trait.
 */
object Reader {
  val Q_GET_SELF_ID_STMT = "select * from self"
  val Q_NODE_BY_ID_STMT = "select * from node where id=?"
  val Q_ALL_NODES_STMT = "select * from node"
  val Q_NODES_BY_BUCKET_STMT = "select * from node where id>=? and id<=?"
  val Q_BUCKET_BY_ID_STMT = "select * from bucket where bucket.id<=? order by bucket.id desc limit 1"
  val Q_ALL_BUCKETS_STMT = "select * from bucket"
  val Q_ALL_PEERS_STMT = "select * from peer where infohash=?"
}

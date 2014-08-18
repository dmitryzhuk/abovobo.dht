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

import java.net.{InetAddress, InetSocketAddress}

import org.abovobo.dht.{Bucket, NodeInfo}
import org.abovobo.dht.message.Message
import org.abovobo.integer.Integer160
import org.abovobo.jdbc.Closer._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

/**
 * Unit test for H2 PermanentlyConnectedStorage implementation
 */
class PermanentlyConnectedStorageTest extends WordSpec with Matchers with BeforeAndAfterAll {

  // initialize DataSource instance
  val ds = DataSource("jdbc:h2:~/db/dht-test-pcs")

  // get instance of connection to be used within a storage
  val connection = this.ds.connection

  // initialize actual Storage instance
  val storage = new PermanentlyConnectedStorage(this.connection)

  /** @inheritdoc */
  override def beforeAll() = {

    // drop everything in the beginning and create working schema
    using(this.ds.connection) { c =>
      using(c.createStatement()) { s =>
        s.execute("drop all objects")
        s.execute("create schema ipv4")
        c.commit()
      }
    }

    // set working schema for the storage
    this.storage.setSchema("ipv4")

    // create common tables
    this.storage.execute("/tables-common.sql")

    // create tables specific to IPv4 protocol which will be used in test
    this.storage.execute("/tables-ipv4.sql")
  }

  /** @inheritdoc */
  override def afterAll() = {
    this.connection.commit()
    this.storage.close()
    this.connection.close()
    this.ds.close()
  }

  "A storage" when {

    "just created" must {
      "have no ID, no nodes and single bucket covering the whole address space" in {
        this.storage.id() should be (None)
        this.storage.nodes() should have size 0
        val buckets = this.storage.buckets()
        buckets should have size 1
        buckets.head.start should be (Integer160.zero)
        buckets.head.end should be (Integer160.maxval)
      }
    }

    "bucket touched" must {
      "have last seen time stamp not older than 10 millseconds" in {
        this.storage.touch(Integer160.zero)
        (System.currentTimeMillis() - this.storage.buckets().head.seen.getTime).toInt should be < 10
      }
    }

    "another bucket inserted" must {
      "have bucket collection size of 2" in {
        this.storage.insert(Integer160.zero + 2)
        this.storage.buckets() should have size 2
      }
      "have second bucket id equal to inserted bucket" in {
        this.storage.buckets().head.end should be(Integer160.zero + 1)
        this.storage.buckets().last.start should be(Integer160.zero + 2)
      }
      "and 100 ms sleep must provide last seen time stamp must be older than 99 ms for that bucket" in {
        Thread.sleep(100)
        (System.currentTimeMillis() - this.storage.buckets().last.seen.getTime).toInt should be > 99
      }
    }

    "node inserted into a storage" must {
      "have node collection size of 1" in {
        this.storage.insert(
          new NodeInfo(Integer160.maxval, new InetSocketAddress(0)),
          Message.Kind.Query)
        this.storage.nodes() should have size 1
      }
      "have size of the bucket equal to 1" in {
        val bucket = this.storage.bucket(Integer160.maxval)
        bucket.start should be (Integer160.zero + 2)
        this.storage.nodes(bucket) should have size 1
      }
      "provide node instance by its id" in {
        this.storage.node(Integer160.maxval) should be('defined)
      }
    }

    "node updated" must {
      "update replied timestamp with Reply message kind" in {
        val original = this.storage.node(Integer160.maxval)
        assume(original.isDefined)
        val node = new NodeInfo(original.get.id, original.get.address)
        this.storage.update(node, original.get, Message.Kind.Response)
        val updated = this.storage.node(Integer160.maxval)
        assume(updated.isDefined)
        assume(updated.get.replied.isDefined)
        (updated.get.replied.get.getTime - System.currentTimeMillis()).toInt should be < 5
      }
      "update replied timestamp with Error message kind" in {
        val original = this.storage.node(Integer160.maxval)
        assume(original.isDefined)
        val node = new NodeInfo(original.get.id, original.get.address)
        this.storage.update(node, original.get, Message.Kind.Error)
        val updated = this.storage.node(Integer160.maxval)
        assume(updated.isDefined)
        assume(updated.get.replied.isDefined)
        (updated.get.replied.get.getTime - System.currentTimeMillis()).toInt should be < 5
      }
      "update queried timestamp with Query message kind" in {
        val original = this.storage.node(Integer160.maxval)
        assume(original.isDefined)
        val node = new NodeInfo(original.get.id, original.get.address)
        this.storage.update(node, original.get, Message.Kind.Query)
        val updated = this.storage.node(Integer160.maxval)
        assume(updated.isDefined)
        assume(updated.get.replied.isDefined)
        (updated.get.queried.get.getTime - System.currentTimeMillis()).toInt should be < 5
      }
      "increment failcount with Fail message kind" in {
        val original = this.storage.node(Integer160.maxval)
        assume(original.isDefined)
        val node = new NodeInfo(original.get.id, original.get.address)
        this.storage.update(node, original.get, Message.Kind.Fail)
        val updated = this.storage.node(Integer160.maxval)
        assume(updated.isDefined)
        (updated.get.failcount - original.get.failcount) should be(1)
      }
      "update address" in {
        val ip = new InetSocketAddress(InetAddress.getByAddress(Array[Byte](1, 0, 0, 0)), 1)
        val node = new NodeInfo(Integer160.maxval, ip)
        val original = this.storage.node(Integer160.maxval)
        assume(original.isDefined)
        this.storage.update(node, original.get, Message.Kind.Error)
        val updated = this.storage.node(Integer160.maxval)
        assume(updated.isDefined)
        updated.get.address should equal (ip)
      }

    }

    "node deleted" must {
      "have empty node collection" in {
        this.storage.delete(Integer160.maxval)
        this.storage.nodes() should have size 0
      }
      "do not provide node instance by its id" in {
        this.storage.node(Integer160.maxval) should be('empty)
      }
      "have empty bucket" in {
        this.storage.nodes(new Bucket(Integer160.zero + 2, Integer160.maxval, new java.util.Date())) should have size 0
      }
    }

    "data is dropped" must {
      "have bucket collection containing only zeroth bucket and no nodes" in {
        this.storage.insert(
          new NodeInfo(Integer160.maxval, new InetSocketAddress(0)),
          Message.Kind.Query)
        this.storage.nodes() should have size 1
        this.storage.drop()
        this.storage.nodes() should have size 0
        val buckets = this.storage.buckets()
        buckets should have size 1
        buckets.head.start should be(Integer160.zero)
        buckets.head.end should be(Integer160.maxval)
      }
    }

    "when id is changed" must {
      "provide new id" in {
        this.storage.id(Integer160.maxval - 1)
        val id = this.storage.id()
        id should be ('defined)
        id.get should be(Integer160.maxval - 1)
      }
    }
  }

}

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
import java.sql.SQLException

import org.abovobo.dht.Node
import org.abovobo.dht.message.Message
import org.abovobo.integer.Integer160
import org.abovobo.jdbc.Closer._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

/**
 * Unit test for H2 Storage implementation
 */
class StorageTest extends WordSpec with Matchers with BeforeAndAfterAll {

  val ds = using(this.getClass.getResourceAsStream("/tables.sql")) { is: java.io.InputStream =>
    using(new java.io.InputStreamReader(is)) { reader =>
      DataSource("jdbc:h2:~/db/dht", reader).close()
    }
    DataSource("jdbc:h2:~/db/dht;SCHEMA=ipv4")
  }

  val writer = new Writer(this.ds.connection)
  val reader = new Reader(this.ds.connection)

  override def beforeAll() = {
  }

  override def afterAll() = {
    this.writer.commit()
    this.writer.close()
    this.reader.close()
    this.ds.close()
  }

  "A storage" when {

    "dropped" must {
      "be empty" in {
        this.writer.drop()
        this.reader.buckets() should have size 0
        this.reader.nodes() should have size 0
      }
    }

    "node is being inserted in empty storage" must {
      "fail with SQLException" in {
        val node = new Node(Integer160.random, new InetSocketAddress(0))
        intercept[SQLException] {
          this.writer.insert(node, Integer160.zero, Message.Kind.Query)
        }
      }
    }

    "bucket inserted in empty storage" must {
      "have bucket collection size of 1" in {
        this.writer.insert(Integer160.zero)
        this.reader.buckets() should have size 1
      }
      "have first bucket id equal to inserted bucket" in {
        this.reader.buckets().head._1 should be(Integer160.zero)
      }
      "and 100 ms sleep must provide last seen time stamp must be older than 99 ms for that bucket" in {
        Thread.sleep(100)
        (System.currentTimeMillis() - this.reader.buckets().head._2.getTime).toInt should be > 99
      }
    }

    "bucket touched" must {
      "have last seen time stamp not older than 10 millseconds" in {
        this.writer.touch(Integer160.zero)
        (System.currentTimeMillis() - this.reader.buckets().head._2.getTime).toInt should be < 10
      }
    }

    "another bucket inserted" must {
      "have bucket collection size of 2" in {
        this.writer.insert(Integer160.zero + 1)
        this.reader.buckets() should have size 2
      }
    }

    "node inserted into a bucket" must {
      "have node collection size of 1" in {
        this.writer.insert(
          new Node(Integer160.maxval, new InetSocketAddress(0)),
          Integer160.zero,
          Message.Kind.Query)
        this.reader.nodes() should have size 1
      }
      "have size of the bucket equal to 1" in {
        this.reader.bucket(Integer160.zero) should have size 1
      }
      "provide node instance by its id" in {
        this.reader.node(Integer160.maxval) should be('defined)
      }
    }

    "node moved to another bucket" must {
      "have size of the first bucket equal to 0 and size of the second bucket equal to 1" in {
        val node = this.reader.node(Integer160.maxval)
        val buckets = this.reader.buckets().toIndexedSeq
        assume(node.isDefined)
        assume(buckets.size == 2)
        this.writer.move(node.get, Integer160.zero + 1)
        this.reader.bucket(buckets(0)._1) should have size 0
        this.reader.bucket(buckets(1)._1) should have size 1
      }
    }

    "node updated" must {
      "update replied timestamp with Reply message kind" in {
        Thread.sleep(10)
        val original = this.reader.node(Integer160.maxval)
        assume(original.isDefined)
        val node = new Node(original.get.id, original.get.address)
        this.writer.update(node, original.get, Message.Kind.Response)
        val updated = this.reader.node(Integer160.maxval)
        assume(updated.isDefined)
        assume(updated.get.replied.isDefined)
        (updated.get.replied.get.getTime - System.currentTimeMillis()).toInt should be < 5
      }
      "update replied timestamp with Error message kind" in {
        Thread.sleep(10)
        val original = this.reader.node(Integer160.maxval)
        assume(original.isDefined)
        val node = new Node(original.get.id, original.get.address)
        this.writer.update(node, original.get, Message.Kind.Error)
        val updated = this.reader.node(Integer160.maxval)
        assume(updated.isDefined)
        assume(updated.get.replied.isDefined)
        (updated.get.replied.get.getTime - System.currentTimeMillis()).toInt should be < 5
      }
      "update queried timestamp with Query message kind" in {
        Thread.sleep(10)
        val original = this.reader.node(Integer160.maxval)
        assume(original.isDefined)
        val node = new Node(original.get.id, original.get.address)
        this.writer.update(node, original.get, Message.Kind.Query)
        val updated = this.reader.node(Integer160.maxval)
        assume(updated.isDefined)
        assume(updated.get.replied.isDefined)
        (updated.get.queried.get.getTime - System.currentTimeMillis()).toInt should be < 5
      }
      "increment failcount with Fail message kind" in {
        val original = this.reader.node(Integer160.maxval)
        assume(original.isDefined)
        val node = new Node(original.get.id, original.get.address)
        this.writer.update(node, original.get, Message.Kind.Fail)
        val updated = this.reader.node(Integer160.maxval)
        assume(updated.isDefined)
        (updated.get.failcount - original.get.failcount) should be(1)
      }
      "update address" in {
        val ip = new InetSocketAddress(InetAddress.getByAddress(Array[Byte](1, 0, 0, 0)), 1)
        val node = new Node(Integer160.maxval, ip)
        val original = this.reader.node(Integer160.maxval)
        assume(original.isDefined)
        this.writer.update(node, original.get, Message.Kind.Error)
        val updated = this.reader.node(Integer160.maxval)
        assume(updated.isDefined)
        updated.get.address should equal (ip)
      }

    }

    "node deleted" must {
      "have empty node collection" in {
        this.writer.delete(Integer160.maxval)
        this.reader.nodes() should have size 0
      }
      "do not provide node instance by its id" in {
        this.reader.node(Integer160.maxval) should be('empty)
      }
      "have empty bucket" in {
        this.reader.bucket(Integer160.zero) should have size 0
      }
    }

    "when id is changed" must {
      "provide new id" in {
        this.writer.id(Integer160.maxval - 1)
        val id = this.reader.id()
        id should be ('defined)
        id.get should be(Integer160.maxval - 1)
      }
    }
  }

}

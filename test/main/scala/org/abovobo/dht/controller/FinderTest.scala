/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.controller

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.abovobo.integer.Integer160
import org.abovobo.dht.Node
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
 * Unit test for [[Finder]]
 */
class FinderTest(system: ActorSystem)
  extends TestKit(system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("FinderTest", ConfigFactory.parseString("akka.loglevel=debug")))

  val target = Integer160.random
  val seed = new Node(Integer160.random, new InetSocketAddress(0))
  val finder = new Finder(target, 8, 3, List(seed))

  override def beforeAll() {

    // distance to seed
    val d = this.target ^ this.seed.id
    // lower distance bound
    val l = d / 1000

    val o = new NodeOrdering(this.target)

    println()
    println(l < d)
    println(o.compare(new Node(l ^ this.target, new InetSocketAddress(0)), new Node(this.seed.id, new InetSocketAddress(0))))

    val r = l + Integer160.random % (d - l)
    println(r < d)
    println(o.compare(new Node(r ^ this.target, new InetSocketAddress(0)), new Node(this.seed.id, new InetSocketAddress(0))))

  }

  override def afterAll() {}

  "Finder instance" when {
    "has just been created" must {
      "have state Finder.State.Continue" in {

        this.finder.state should be(Finder.State.Continue)

      }
    }

    "nodes has been taken for the first time" must {
      "have state Finder.State.Wait" in {

        val more = this.finder.take(3)
        more.size should be (1)
        finder.state should be(Finder.State.Wait)

      }
    }

    "got report with valid nodes from the initial seed" must {
      "have state Finder.State.Continue" in {

        val distanceFromSeed = this.target ^ this.seed.id
        val lowerDistanceBound = distanceFromSeed / 1000
        val distanceRange = distanceFromSeed - lowerDistanceBound
        val ids = for (i <- 0 until 8) yield (lowerDistanceBound + Integer160.random % distanceRange) ^ this.target
        val nodes = ids.map(new Node(_, new InetSocketAddress(0)))
        // -----
        this.finder.report(seed, nodes, Nil, Array.empty)
        this.finder.state should be(Finder.State.Continue)
      }
    }

    "given alpha nodes and got report with even closer nodes form them" must {
      "have state Finder.State.Continue after first node reported" in {
        val more = this.finder.take(3)
        val reporter = more.take(1).head
        val distanceFromReporter = this.target ^ reporter.id
        val lowerDistanceBound = distanceFromReporter / 1000
        val distanceRange = distanceFromReporter - lowerDistanceBound
        val ids = for (i <- 0 until 8) yield (lowerDistanceBound + Integer160.random % distanceRange) ^ this.target
        val nodes = ids.map(new Node(_, new InetSocketAddress(0)))
        // -----
        this.finder.report(reporter, nodes, Nil, Array.empty)
        this.finder.state should be(Finder.State.Continue)
      }
    }
  }

}

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
  val seed = new Node(Integer160.zero, new InetSocketAddress(0))
  val finder = new Finder(this.target, 8, 3, List(this.seed))

  override def beforeAll() {}

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

    val distanceFromSeed = this.target ^ this.seed.id
    var k = 0

    "got report with valid nodes from the initial seed" must {
      "have state Finder.State.Continue" in {

        val ids = for (i <- 0 until 8) yield (distanceFromSeed - (8 * k) - (i + 1)) ^ this.target
        k += 1
        val nodes = ids.map(new Node(_, new InetSocketAddress(0)))
        // -----
        this.finder.report(seed, nodes, Nil, Array.empty)
        this.finder.state should be(Finder.State.Continue)
      }
    }

    "given alpha nodes and got report with even closer nodes form them" must {
      "have state Finder.State.Continue after taken nodes reported" in {
        this.finder.take(3) foreach { reporter =>
          val ids = for (i <- 0 until 8) yield (distanceFromSeed - (8 * k) - (i + 1)) ^ this.target
          k += 1
          val nodes = ids.map(new Node(_, new InetSocketAddress(0)))
          // -----
          this.finder.report(reporter, nodes, Nil, Array.empty)
          this.finder.state should be(Finder.State.Continue)
        }
      }
    }

    "given alpha nodes and got report" must {
      "have state Finder.State.Wait after the first node reported no closer nodes and Finder.State.Finalize after all round reported no closer nodes" in {
        val more = this.finder.take(3).toArray
        k = 0
        def report(node: Node) {
          val ids = for (i <- 0 until 8) yield (distanceFromSeed + (8 * k) + (i + 1)) ^ this.target
          k += 1
          val nodes = ids.map(new Node(_, new InetSocketAddress(0)))
          this.finder.report(node, nodes, Nil, Array.empty)
        }
        for (i <- 0 until 2) {
          report(more(i))
          this.finder.state should be(Finder.State.Wait)
        }
        report(more(2))
        this.finder.state should be(Finder.State.Finalize)
      }
    }

    "given K nodes and got report" must {
      "have state Finder.State.Wait after while K nodes reporting no closer nodes and Finder.State.Succeeded after round is completed" in {
        while (this.finder.state == Finder.State.Finalize) {
          val more = this.finder.take(8).toArray
          def report(node: Node) {
            val ids = for (i <- 0 until 8) yield (distanceFromSeed + (8 * k) + (i + 1)) ^ this.target
            k += 1
            val nodes = ids.map(new Node(_, new InetSocketAddress(0)))
            this.finder.report(node, nodes, Nil, Array.empty)
          }
          for (i <- 0 until 7) {
            report(more(i))
            this.finder.state should be(Finder.State.Wait)
          }
          report(more(7))
        }
        this.finder.state should be(Finder.State.Succeeded)
      }
    }
  }

}

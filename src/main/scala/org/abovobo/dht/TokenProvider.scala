/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.ExecutionContext
import akka.actor.Scheduler
import org.abovobo.integer.Integer160

/**
 * This class is responsible for providing tokens and checking token validity.
 */
class TokenProvider {

  /** Returns current token */
  def get: Token = this.tokens(0)

  /**
   * Checks if given token is valid (e.g. contains in private array of valid tokens).
   *
   * @param token A token to check.
   * @return      `true` if given token is valid.
   */
  def valid(token: Token): Boolean = this.tokens(0).sameElements(token) || this.tokens(1).sameElements(token)

  /** Rotates array of tokens generating new one */
  def rotate() = {
    this.tokens(1) = this.tokens(0)
    this.tokens(0) = Integer160.random.toArray
  }

  /** Array of currently valid tokens. */
  private val tokens: Array[Token] = Array[Token](Integer160.zero.toArray, Integer160.zero.toArray)
}



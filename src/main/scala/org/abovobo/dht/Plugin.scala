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

import akka.actor.Actor

/**
 * Represents Plugin identifier. Actually, this is just a wrapper over the Long integer type.
 *
 * @param value Value representing Plugin identifier.
 */
class PID(val value: Long) {
  /// Check that value actually fits in Byte
  if (value < 0 || value > 255) {
    throw new IllegalArgumentException()
  }

  /** @inheritdoc */
  override def toString = this.value.toString
}
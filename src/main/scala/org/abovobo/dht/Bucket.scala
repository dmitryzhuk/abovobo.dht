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

import org.abovobo.integer.Integer160

/**
 * This class defines routing table bucket properties.
 *
 * @param start Lower bound of the bucket range. This is inclusive.
 * @param end   Upper bound of the bucket range. This is inclusive.
 * @param seen  Last time an activity related to this bucket has been seen.
 */
class Bucket(val start: Integer160, val end: Integer160, val seen: java.util.Date) {

  /**
   * Checks if given id falls within the range of the bucket.
   *
   * @param id An id to test.
   * @return   True if given id is within the range of the bucket.
   */
  def in(id: Integer160): Boolean = this.start <= id && id <= this.end
}

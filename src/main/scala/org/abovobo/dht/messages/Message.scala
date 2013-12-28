/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.messages

/**
 * Basic class which defines message type.
 *
 * It effectively 'flattens' multilevel structure of backend Map collection and represents properties via member
 * accessor functions.
 *
 * @param backend
 * 			A TreeMap instance which backs this object
 */
abstract class Message {

  /**
   * Returns transaction identifier.
   * Represents 't' key of bencoded message dictionary.
   *
   * @return transasaction identifier representing 't' key of bencoded dictionary.
   */
  def tid: Array[Byte]

  /**
   * Returns single-character message type identifier.
   * Represents 'y' key of bencoded message dictionary.
   *
   * @return message type identifier representing 'y' key of bencoded dictionary.
   */
  def kind: Char

}

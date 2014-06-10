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

/**
 * Represents transaction id.
 *
 * @param value transaction identifier in form of byte array
 */
class TID(private val value: Array[Byte]) {

  /**
   * Returns byte array representation of transaction identifier.
   *
   * @return byte array representation of transaction identifier.
   */
  def toArray = this.value

  /** Returns stringified value of backend bytearray */
  override def toString: String = new String(this.value, "UTF-8")

  /** Overrides standard Java equals delegating operation to underlying value */
  override def equals(obj: Any): Boolean = obj match {
    case tid: TID => this.value.deep == tid.value.deep
    case _ => false
  }

  /** Overrides standard Java hashCode delegating operation to underlying value */
  override def hashCode: Int = this.toString.hashCode
}

/** Factory which creates instances of TID in loop. */
class TIDFactory {
  import TIDFactory.alfabet 

  /// Index of the first byte of TID
  private var i0 = 0

  /// Index of the second byte of TID
  private var i1 = 0

  /** Generates the next transaction id. */
  def next(): TID = {
    val result = new TID(Array(alfabet(i0), alfabet(i1)))
    i1 += 1
    if (i1 == alfabet.length) {
      i0 += 1
      i1 = 0
      if (i0 == alfabet.length) {
        i0 = 0
      }
    }
    result
  }
}

object TIDFactory {  
  /// An array of characters which can be used to generate transaction identifiers
  private val alfabet = Array[Byte](
    '0','1','2','3','4','5','6','7','8','9', 
    'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'//,
//    'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'
  )
  
  def random: TIDFactory = {
    val f = new TIDFactory
    f.i0 = (System.currentTimeMillis % alfabet.length).toInt
    f.i1 = (f.hashCode % alfabet.length).toInt
    f
  }
}
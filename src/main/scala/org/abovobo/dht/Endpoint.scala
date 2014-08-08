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

import java.net.{InetAddress, InetSocketAddress}
import org.abovobo.conversions.{Unsigned, ByteArray}

/**
 * This object defines implicit conversion rules allowing to serialize
 * [[java.net.InetSocketAddress]] into a byte array and then deserialize
 * it back.
 */
object Endpoint {

  import scala.language.implicitConversions

  /** Size in bytes of IPv4 address */
  val IPV4_ADDR_SIZE = 4

  /** Size in bytes of IPv6 address */
  val IPV6_ADDR_SIZE = 16

  /**
   * Converts [[java.net.InetSocketAddress]] into a byte array.
   *
   * @param address An address to convert
   * @return        Byte array representing given [[java.net.InetSocketAddress]]
   */
  implicit def isa2ba(address: InetSocketAddress): Array[Byte] =
    address.getAddress.getAddress ++ ByteArray.short2ba(address.getPort.toShort)

  /**
   * Constructs [[java.net.InetSocketAddress]] from given byte array.
   * The result will contain IPv4 or IPv6 address depending on given
   * byte array length. If byte array length does not conform one of 2
   * possible values, [[java.lang.IllegalArgumentException]] will be
   * thrown.
   *
   * @param ba An array of bytes to create [[java.net.InetSocketAddress]] instance from.
   * @return   Instance of [[java.net.InetSocketAddress]].
   * @throws IllegalArgumentException if length of given byte array does not conform niether
   *                                  IPv4 nor IPv6 address size.
   */
  implicit def ba2isa(ba: Array[Byte]): InetSocketAddress =
    if (ba.length == IPV4_ADDR_SIZE + 2) {
      new InetSocketAddress(
        InetAddress.getByAddress(ba.take(IPV4_ADDR_SIZE)),
        Unsigned.uint(ByteArray.ba2short(ba.drop(IPV4_ADDR_SIZE))))
    } else if (ba.length == IPV6_ADDR_SIZE + 2) {
      new InetSocketAddress(
        InetAddress.getByAddress(ba.take(IPV6_ADDR_SIZE)),
        Unsigned.uint(ByteArray.ba2short(ba.drop(IPV6_ADDR_SIZE))))
    } else {
      // 58 bytes?
      throw new IllegalArgumentException("Invalid length of a byte array: " + ba.length)
    }

  /**
   * The same as [[org.abovobo.dht.Endpoint#ba2isa]] but operates with
   * [[scala.Option]] wrapped objects.
   *
   * @param oba An array of bytes to create [[java.net.InetSocketAddress]] instance from or None.
   * @return    Instance of [[java.net.InetSocketAddress]] or None.
   * @throws IllegalArgumentException if length of given byte array does not conform niether
   *                                  IPv4 nor IPv6 address size.
   */
  implicit def oba2oisa(oba: Option[Array[Byte]]): Option[InetSocketAddress] = oba.map(this.ba2isa)
}

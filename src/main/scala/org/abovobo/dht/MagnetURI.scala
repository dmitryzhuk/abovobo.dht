/**
 * Abovobo Bittorrent Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht

/**
 * This class represents Magnet URI. Parameters supposed to be in human readable form (no URI encoding)
 *
 * @param parameters Magnet URI parameters
 */
class MagnetURI(private val parameters: Map[String, String]) {

  /**
   * Constructs Magnet from string.
   *
   * @param s String value to parse Magnet URI from.
   */
  def this(s: String) = this(MagnetURI.parse(s))

  /**
   * Simply returns internal representation of Magnet URI (already encoded)
   */
  override def toString: String = this._s

  /** Compares string representations of two Magnets */
  override def equals(that: Any): Boolean = that match {
    case same: MagnetURI => this.toString.equals(same.toString)
    case _ => false
  }

  /** Hash code is built directly from string representation of this Magnet */
  override def hashCode: Int = this.toString.hashCode

  /// String representation of this Magnet.
  /// Note that if instance is constructed from string decoding of the whole string
  /// and then decoding back of every separate component will be done thus this
  /// representation may differ from original string passed into constructor.
  private val _s: String = "magnet:?" + this.parameters.map(p =>
      java.net.URLEncoder.encode(p._1, MagnetURI.encoding) + "=" + java.net.URLEncoder.encode(p._2, MagnetURI.encoding)
  ).reduceLeft {
    _ + "&" + _
  }

}


object MagnetURI {

  /** This object encapsulates pre-defined query keys in magnet URI */
  object Key {
    val ExactTopic = "xt"
    val ExactLength = "xl"
    val DisplayName = "dn"
    val AcceptableSource = "as"
    val ExactSource = "xs"
    val KeywordTopic = "kt"
    val ManifestTopic = "mt"
    val AddressTracker = "tr"
  }

  /// Parses URI string producing map of query parameters. URI is validated during parsing
  private def parse(s: String): Map[String, String] = s.indexOf(":") match {

    // Colon character not found which means that scheme is not present
    case -1 => throw new java.net.URISyntaxException(s, "Scheme is mandatory for magnet", 0)

    // Colon character found
    case index: Int =>

      // Scheme part is not magnet
      if (s.substring(0, index) != "magnet") {
        throw new java.net.URISyntaxException(s, "Scheme must be magnet", index)
      }

      // Scheme is not followed by non empty query
      else if (s.length < index + 3) {
        throw new java.net.URISyntaxException(s, "Query part must be presented", index + 1)
      }

      // The very first character after scheme is not query beginning character
      else if (s.charAt(index + 1) != '?') {
        throw new java.net.URISyntaxException(s, "Query part must be presented", index + 1)
      }

      // Scheme is valid magnet one and non-empty query string is here, so cut off actual query body
      // and split it into map of named values
      else {
        s.substring(index + 2).split("&").map(
          _.split("=") match {
            case array: Array[String] =>
              if (array.length == 2) {
                java.net.URLDecoder.decode(array(0), MagnetURI.encoding) ->
                  java.net.URLDecoder.decode(array(1), MagnetURI.encoding)
              }
              else if (array.length == 1) {
                java.net.URLDecoder.decode(array(0), MagnetURI.encoding) -> ""
              }
              else {
                throw new java.net.URISyntaxException(s, "Failed to parse query parameter")
              }
            case _ => throw new java.net.URISyntaxException(s, "Failed to parse query parameter")
          }
        ).toMap
      }
  }

  /// This encoding is used when decoding URI components
  private val encoding = "UTF-8"
}


package com.thatdot.quine.util

import java.net.InetAddress

final case class Host(asString: String) extends AnyVal
final case class Port(asInt: Int) extends AnyVal
object Config {

  def replaceHostSpecialValues(s: String): String = s match {
    // These special cased hostnames match the special cases in pekko's ArterySettings:
    // This allows using pekko-style <get...> syntax in Quine's config
    case "<getHostAddress>" => InetAddress.getLocalHost.getHostAddress
    case "<getHostName>" => InetAddress.getLocalHost.getHostName
    case x => x
  }

  def replacePortSpecialValue(i: Int): Int = i match {
    case 0 => LoopbackPort()
    case x => x
  }

}

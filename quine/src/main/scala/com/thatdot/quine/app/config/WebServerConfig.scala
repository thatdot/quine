package com.thatdot.quine.app.config

import java.io.File
import java.net.{InetAddress, URL}

import org.apache.pekko.http.scaladsl.model.Uri

import com.thatdot.quine.app.config.WebServerBindConfig.{KeystorePasswordEnvVar, KeystorePathEnvVar}
import com.thatdot.quine.util.{Host, Port}

final case class SslConfig(path: File, password: Array[Char])

final case class WebServerBindConfig(
  address: Host = Host("0.0.0.0"),
  port: Port = Port(8080),
  enabled: Boolean = true,
  useTls: Boolean = sys.env.contains(KeystorePathEnvVar) && sys.env.contains(KeystorePasswordEnvVar),
) {
  def protocol: String = if (useTls) "https" else "http"

  def guessResolvableUrl: URL = {
    val bindHost: Uri.Host = Uri.Host(address.asString)
    // If the host of the bindUri is set to wildcard (INADDR_ANY and IN6ADDR_ANY) - i.e. "0.0.0.0" or "::"
    // present the URL as "localhost" to the user. This is necessary because while
    // INADDR_ANY as a source address means "bind to all interfaces", it cannot necessarily be
    // used as a destination address
    val resolveableHost =
      if (bindHost.inetAddresses.head.isAnyLocalAddress)
        Uri.Host(InetAddress.getLoopbackAddress)
      else
        bindHost

    new URL(protocol, resolveableHost.address, port.asInt, "")
  }

}
object WebServerBindConfig {
  val KeystorePathEnvVar = "SSL_KEYSTORE_PATH"
  val KeystorePasswordEnvVar = "SSL_KEYSTORE_PASSWORD"
}
final case class WebserverAdvertiseConfig(
  address: Host,
  port: Port,
  path: Option[String] = None,
) {
  def url(protocol: String): URL =
    new URL(protocol, address.asString, port.asInt, path.getOrElse(""))
}

package com.thatdot.quine.app.config

import java.io.File
import java.net.InetAddress

import akka.http.scaladsl.model.Uri

import com.thatdot.quine.util.{Host, Port}

final case class SslConfig(path: File, password: Array[Char])

trait WebServerConfig {
  def address: Host
  def port: Port
  def ssl: Option[SslConfig]
}
final case class WebServerBindConfig(
  address: Host,
  port: Port,
  enabled: Boolean = true,
  ssl: Option[SslConfig] = (sys.env.get("SSL_KEYSTORE_PATH"), sys.env.get("SSL_KEYSTORE_PASSWORD")) match {
    case (None, None) => None
    case (Some(path), Some(password)) => Some(SslConfig(new File(path), password.toCharArray))
    case (Some(_), None) => sys.error("'SSL_KEYSTORE_PATH' was specified but 'SSL_KEYSTORE_PASSWORD' was not")
    case (None, Some(_)) => sys.error("'SSL_KEYSTORE_PASSWORD' was specified but 'SSL_KEYSTORE_PATH'  was not")
  }
) extends WebServerConfig {

  val asResolveableUrl: Uri = {
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

    Uri(if (ssl.isDefined) "https" else "http", Uri.Authority(resolveableHost, port.asInt))
  }
}
final case class WebserverAdvertiseConfig(
  address: Host,
  port: Port
) {
  def overrideHostAndPort(uri: Uri): Uri = uri.withHost(address.asString).withPort(port.asInt)
}

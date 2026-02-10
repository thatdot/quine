package com.thatdot.quine.app.config

import java.io.File
import java.net.{InetAddress, URL}

import org.apache.pekko.http.scaladsl.model.Uri

import pureconfig.generic.semiauto.deriveConvert
import pureconfig.{ConfigConvert, ConfigReader, ConfigWriter}

import com.thatdot.quine.app.config.WebServerBindConfig.{KeystorePasswordEnvVar, KeystorePathEnvVar}
import com.thatdot.quine.util.{Host, Port}

final case class SslConfig(path: File, password: Array[Char])

object SslConfig extends PureconfigInstances {
  implicit val configConvert: ConfigConvert[SslConfig] = {
    implicit val charArrayReader: ConfigReader[Array[Char]] = QuineConfig.charArrayReader
    implicit val charArrayWriter: ConfigWriter[Array[Char]] = QuineConfig.charArrayWriter
    deriveConvert[SslConfig]
  }
}

final case class MtlsTrustStore(path: File, password: String)

object MtlsTrustStore extends PureconfigInstances {
  implicit val configConvert: ConfigConvert[MtlsTrustStore] = deriveConvert[MtlsTrustStore]
}

final case class MtlsHealthEndpoints(
  enabled: Boolean = false,
  port: Port = Port(8081),
)

object MtlsHealthEndpoints extends PureconfigInstances {
  implicit val configConvert: ConfigConvert[MtlsHealthEndpoints] = deriveConvert[MtlsHealthEndpoints]
}

final case class UseMtls(
  enabled: Boolean = false,
  trustStore: Option[MtlsTrustStore] = None,
  healthEndpoints: MtlsHealthEndpoints = MtlsHealthEndpoints(),
)

object UseMtls extends PureconfigInstances {
  implicit val configConvert: ConfigConvert[UseMtls] = deriveConvert[UseMtls]
}

final case class WebServerBindConfig(
  address: Host = Host("0.0.0.0"),
  port: Port = Port(8080),
  enabled: Boolean = true,
  useTls: Boolean = sys.env.contains(KeystorePathEnvVar) && sys.env.contains(KeystorePasswordEnvVar),
  useMtls: UseMtls = UseMtls(),
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
object WebServerBindConfig extends PureconfigInstances {
  val KeystorePathEnvVar = "SSL_KEYSTORE_PATH"
  val KeystorePasswordEnvVar = "SSL_KEYSTORE_PASSWORD"

  implicit val configConvert: ConfigConvert[WebServerBindConfig] = deriveConvert[WebServerBindConfig]
}
final case class WebserverAdvertiseConfig(
  address: Host,
  port: Port,
  path: Option[String] = None,
) {
  def url(protocol: String): URL =
    new URL(protocol, address.asString, port.asInt, path.getOrElse(""))
}

object WebserverAdvertiseConfig extends PureconfigInstances {
  implicit val configConvert: ConfigConvert[WebserverAdvertiseConfig] = deriveConvert[WebserverAdvertiseConfig]
}

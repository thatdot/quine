package com.thatdot.quine.app.config

import java.net.URL

import com.thatdot.quine.util.{Host, Port}

final case class WebServerConfig(
  address: Host,
  port: Port,
  enabled: Boolean = true
) {
  def toURL: URL = new URL("http", address.asString, port.asInt, "")
}

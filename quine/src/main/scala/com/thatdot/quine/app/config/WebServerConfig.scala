package com.thatdot.quine.app.config

import java.net.URL

import com.thatdot.quine.util.{Host, Port}
sealed abstract class WebserverConfig {
  def address: Host
  def port: Port
  def toURL: URL = new URL("http", address.asString, port.asInt, "")
}
final case class WebServerBindConfig(
  address: Host,
  port: Port,
  enabled: Boolean = true
) extends WebserverConfig
final case class WebserverAdvertiseConfig(
  address: Host,
  port: Port
) extends WebserverConfig

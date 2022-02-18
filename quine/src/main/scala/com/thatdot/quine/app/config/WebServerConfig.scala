package com.thatdot.quine.app.config

import java.net.InetAddress

import com.thatdot.quine.util.LoopbackPort

final case class WebServerConfig private (
  address: String,
  port: Int
)
object WebServerConfig {

  def apply(address: String, port: Int): WebServerConfig = {
    val resolvedAddress = address match {
      case "<getHostAddress>" => InetAddress.getLocalHost.getHostAddress
      case "<getHostName>" => InetAddress.getLocalHost.getHostName
      case x => x
    }
    val resolvedPort = port match {
      case 0 => LoopbackPort()
      case x => x
    }

    new WebServerConfig(resolvedAddress, resolvedPort)
  }
}

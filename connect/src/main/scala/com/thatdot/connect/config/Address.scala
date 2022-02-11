package com.thatdot.connect.config

import java.net.InetSocketAddress

import com.google.common.net.HostAndPort

object Address {

  /** Parse an address from an input string
    *
    * @param input string from which to parse the address
    * @param defaultPort if the port is missing, use this port
    * @return parsed address
    */
  def parseHostAndPort(input: String, defaultPort: Int): InetSocketAddress = {
    val hostAndPort = HostAndPort.fromString(input).withDefaultPort(defaultPort)
    InetSocketAddress.createUnresolved(hostAndPort.getHost, hostAndPort.getPort)
  }
}

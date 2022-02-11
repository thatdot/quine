package com.thatdot.quine.util

import java.net.ServerSocket

object LoopbackPort {

  /** @return a new ephemeral port number that is, given a reasonable rate of port allocation, guaranteed
    * to be available for a new listener
    */
  def apply(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }
}

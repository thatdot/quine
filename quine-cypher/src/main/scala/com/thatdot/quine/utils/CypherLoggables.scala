package com.thatdot.quine.utils

import com.thatdot.quine.bolt.Protocol.{
  AckFailure,
  DiscardAll,
  Failure,
  Ignored,
  Init,
  PullAll,
  Record,
  Reset,
  Run,
  Success,
}
import com.thatdot.quine.util.Log._

object CypherLoggables {
  implicit val logProtocolMessage: Loggable[com.thatdot.quine.bolt.Protocol.ProtocolMessage] =
    Loggable[com.thatdot.quine.bolt.Protocol.ProtocolMessage] {
      (msg: com.thatdot.quine.bolt.Protocol.ProtocolMessage, redactor: String => String) =>
        msg match {
          case _: Reset => msg.toString
          case _: Init => redactor(msg.toString)
          case _: Success => msg.toString
          case _: Failure => msg.toString
          case _: AckFailure => msg.toString
          case _: Ignored => msg.toString
          case _: Run => redactor(msg.toString)
          case _: PullAll => msg.toString
          case _: DiscardAll => msg.toString
          case _: Record => redactor(msg.toString)
        }
    }
}

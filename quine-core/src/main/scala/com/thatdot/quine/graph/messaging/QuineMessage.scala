package com.thatdot.quine.graph.messaging

/** Messages relayable across the Quine graph using [[relayTell]] or [[relayAsk]].
  */
abstract class QuineMessage

/** Messages relayable across the Quine graph using [[[[relayAsk]].
  *
  * @tparam Resp expected type of the response to the message
  */
trait AskableQuineMessage[Resp] { self: QuineMessage =>

  /** Location in the Quine graph to which the response should be relayed */
  def replyTo: QuineRef
}

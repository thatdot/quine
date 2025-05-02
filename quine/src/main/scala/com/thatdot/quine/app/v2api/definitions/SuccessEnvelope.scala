package com.thatdot.quine.app.v2api.definitions

sealed trait SuccessEnvelope[+CONTENT]

object SuccessEnvelope {

  final case class Ok[CONTENT](content: CONTENT, message: Option[String] = None, warnings: Option[String] = None)
      extends SuccessEnvelope[CONTENT]

  final case class Created[CONTENT](content: CONTENT, location: Option[String] = None) extends SuccessEnvelope[CONTENT]

  final case class Accepted(
    message: String = "Request accepted. Starting to process task.",
    monitorUrl: Option[String] = None,
  ) extends SuccessEnvelope[Nothing]

  case object NoContent extends SuccessEnvelope[Nothing]
}

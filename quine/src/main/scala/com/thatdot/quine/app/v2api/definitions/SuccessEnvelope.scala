package com.thatdot.quine.app.v2api.definitions

sealed trait SuccessEnvelope[+Content]
sealed trait CreatedOrNoContent[+Content] extends SuccessEnvelope[Content]
object SuccessEnvelope {

  case class Ok[Content](content: Content, message: Option[String] = None, warnings: Option[String] = None)
      extends SuccessEnvelope[Content]

  case class Created[Content](content: Content, message: Option[String] = None, warning: Option[String] = None)
      extends SuccessEnvelope[Content]
      with CreatedOrNoContent[Content]

  case class Accepted(
    message: String = "Request accepted. Starting to process task.",
    monitorUrl: Option[String] = None,
  ) extends SuccessEnvelope[Nothing]

  case object NoContent extends SuccessEnvelope[Nothing] with CreatedOrNoContent[Nothing]
}

package com.thatdot.api.v2

import sttp.tapir.Schema

sealed trait SuccessEnvelope[+Content]
sealed trait CreatedOrNoContent[+Content] extends SuccessEnvelope[Content]
sealed trait CreatedOrOk[+Content] extends SuccessEnvelope[Content]
object SuccessEnvelope {

  case class Ok[Content](content: Content, message: Option[String] = None, warnings: List[String] = Nil)
      extends SuccessEnvelope[Content]
      with CreatedOrOk[Content]
  object Ok {
    implicit def schema[A](implicit inner: Schema[A]): Schema[Ok[A]] = Schema.derived[Ok[A]]
  }

  case class Created[Content](content: Content, message: Option[String] = None, warnings: List[String] = Nil)
      extends SuccessEnvelope[Content]
      with CreatedOrNoContent[Content]
      with CreatedOrOk[Content]
  object Created {
    implicit def schema[A](implicit inner: Schema[A]): Schema[Created[A]] = Schema.derived[Created[A]]
  }

  case class Accepted(
    message: String = "Request accepted. Starting to process task.",
    monitorUrl: Option[String] = None,
  ) extends SuccessEnvelope[Nothing]
  object Accepted {
    implicit val schema: Schema[Accepted] = Schema.derived[Accepted]
  }

  case object NoContent extends SuccessEnvelope[Nothing] with CreatedOrNoContent[Nothing] {
    implicit val schema: Schema[NoContent.type] = Schema.derived[NoContent.type]
  }
}

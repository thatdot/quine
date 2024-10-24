package com.thatdot.quine.app.v2api.definitions

import com.thatdot.quine.util.BaseError

sealed trait CustomError
case class ServerError(errors: String) extends CustomError
case class BadRequest(errors: List[String]) extends CustomError
case class NotFound(errors: String) extends CustomError
case class Unauthorized(errors: String) extends CustomError
case class ServiceUnavailable(errors: String) extends CustomError
case object NoContent extends CustomError

object BadRequest {
  def apply(error: String): BadRequest = BadRequest(List(error))
  def apply(error: BaseError): BadRequest = BadRequest(List(error.getMessage))
  def ofErrors(errors: List[BaseError]): BadRequest = BadRequest(errors.map(_.getMessage))
}
object CustomError {

  /** Placeholder. Should be (throwable -> CustomError) */
  def toCustomError(e: Throwable): ServerError = ServerError(e.getMessage)
}

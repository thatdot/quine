package com.thatdot.quine.app.v2api.definitions

sealed trait CustomError
case class ServerError(what: String) extends CustomError
case class BadRequest(what: String) extends CustomError
case class NotFound(what: String) extends CustomError
case class Unauthorized(realm: String) extends CustomError
case class Unknown(code: Int, msg: String) extends CustomError
case object NoContent extends CustomError

object CustomError {

  /** Placeholder. Should be (throwable -> CustomError) */
  def toCustomError(e: Throwable): ServerError = ServerError(e.getMessage)
}

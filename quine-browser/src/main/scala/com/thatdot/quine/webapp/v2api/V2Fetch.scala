package com.thatdot.quine.webapp.v2api

import scala.concurrent.{ExecutionContext, Future}

import org.scalajs.dom

import com.thatdot.quine.webapp.AuthEvents

/** Fetch + JSON-decode for V2 REST endpoints that have no endpoints4s client traits.
  *
  * Resolves URLs honoring the same base URL that endpoints4s uses via
  * `ClientRoutes.baseUrlOpt`. When the base URL is set (typically derived by the
  * startup JS to absorb any reverse-proxy path prefix), it's prepended; otherwise
  * the path is left relative so the browser resolves it against the current
  * document — which preserves the proxy prefix automatically.
  */
object V2Fetch {

  /** Fetch `path` (relative, no leading slash) and decode the JSON response body.
    *
    * A 401 emits [[AuthEvents.unauthorized]]; any network, HTTP, or decode failure
    * yields a failed future with a user-facing message.
    */
  def apply[A: io.circe.Decoder](
    path: String,
    baseUrlOpt: Option[String],
  )(implicit ec: ExecutionContext): Future[A] = {
    val url = baseUrlOpt match {
      case Some(base) => s"${base.stripSuffix("/")}/$path"
      case None => path
    }
    dom
      .fetch(url)
      .toFuture
      .recoverWith { case _ =>
        Future.failed(new RuntimeException("Cannot reach server"))
      }
      .flatMap { response =>
        if (response.status == 401) {
          AuthEvents.unauthorized.emit(())
          Future.failed(new RuntimeException(s"Server returned ${response.status} ${response.statusText}"))
        } else if (!response.ok) {
          Future.failed(new RuntimeException(s"Server returned ${response.status} ${response.statusText}"))
        } else {
          response.text().toFuture.flatMap { body =>
            io.circe.parser.decode[A](body) match {
              case Right(value) => Future.successful(value)
              case Left(_) => Future.failed(new RuntimeException("Unexpected response from server"))
            }
          }
        }
      }
  }
}

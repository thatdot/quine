package com.thatdot.quine.app.routes.exts

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}

trait ServerRequestTimeoutOps { self: endpoints4s.akkahttp.server.Endpoints =>

  implicit class EndpointOps[A, B](val endpoint: Endpoint[A, B]) {

    /** Similar to `implementedByAsync`, but with more control over timeouts
      *
      * TODO: have some sort of maximum timeout configured at the server level
      *
      * @param requestedTimeout optional override for the request timeout
      * @param implementation how to produce a result
      * @return Akka HTTP route
      */
    def implementedByAsyncWithRequestTimeout(requestedTimeout: A => Option[Duration])(
      implementation: (A, Duration) => Future[B]
    ): Route =
      handleExceptions(ExceptionHandler { case NonFatal(t) => handleServerError(t) }) {
        endpoint.request.directive { arguments =>
          requestedTimeout(arguments).fold(pass)(withRequestTimeout(_)) {
            extractRequestTimeout { actualTimeout =>
              onComplete(implementation(arguments, actualTimeout)) {
                case Success(result) => endpoint.response(result)
                case Failure(ex) => throw ex
              }
            }
          }
        }
      }
  }
}

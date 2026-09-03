package com.thatdot.quine.app.model.ingest.util

import scala.concurrent.Future

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, IllegalUriException}
import org.apache.pekko.stream.scaladsl.{Sink, Source}

object StreamingHttpRequest {

  /** Sends an HTTP request (like `Http().singleRequest`) except the request is sent over a new
    * connection instead of using Pekko's per-host connection pool.
    *
    * This is good for a server-sent events (SSE) connection which keep an HTTP connection open
    * indefinitely. If Pekko's connection pool were used instead, then multiple active SSE
    * connections to the same host would starve out any other HTTP requests to that host.
    */
  def send(request: HttpRequest)(implicit system: ActorSystem): Future[HttpResponse] = {
    val uri = request.uri
    if (uri.scheme.isEmpty || uri.authority.isEmpty)
      Future.failed(
        IllegalUriException(s"Cannot connect: ${request.method.value} request to $uri doesn't have an absolute URI"),
      )
    else {
      val builder = Http().connectionTo(uri.authority.host.toString).toPort(uri.effectivePort)
      val connection = if (uri.scheme.equalsIgnoreCase("https")) builder.https() else builder.http()
      Source.single(request.withUri(uri.toHttpRequestTargetOriginForm)).via(connection).runWith(Sink.head)
    }
  }
}

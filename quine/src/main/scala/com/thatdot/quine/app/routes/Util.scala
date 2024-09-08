package com.thatdot.quine.app.routes

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.headers.{CacheDirectives, RawHeader, `Cache-Control`}
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.server
import org.apache.pekko.http.scaladsl.server.Directives.{complete, respondWithHeader, respondWithHeaders}
import org.apache.pekko.stream.scaladsl.{Flow, Source}

object Util {

  /** Given a stream of ServerSentEvents, produce a pekko-http Route to stream results from behind
    * a reverse proxy (assuming the proxy allows for long-running http/1.1 connections and respects
    * cache headers + X-Accel-Buffering)
    * @see https://serverfault.com/questions/801628/for-server-sent-events-sse-what-nginx-proxy-configuration-is-appropriate
    * @param events the serversentevents stream to lift to a pekko route
    * @return the constructed route
    */
  def sseRoute(events: Source[ServerSentEvent, NotUsed]): server.Route =
    respondWithHeaders(
      `Cache-Control`(CacheDirectives.`no-cache`),
      RawHeader("X-Accel-Buffering", "no"),
    ) { // reverse proxy friendly headers
      // this implicit allows marshalling a Source[ServerSentEvent] to an SSE endpoint
      import org.apache.pekko.http.scaladsl.marshalling.sse.EventStreamMarshalling.toEventStream
      complete {
        events
          // promptly reply with _something_, so the client event stream can be opened
          .prepend(Source.single(ServerSentEvent.heartbeat))
          // pekko defaults to 20sec, firefox's default http request timeout is 15sec
          // most importantly, this keeps reverse proxies from dropping the keepalive connection over http/1.1
          .keepAlive(10.seconds, () => ServerSentEvent.heartbeat)
          .named("sse-server-flow")
      }
    }

  /** Harden the underlying route against XSS by providing a Content Security Policy
    * @param underlying the route to protect
    * @return the augmented route
    *
    * TODO replace this with a better implementation once the issue described in
    * https://github.com/akka/akka-http/issues/155 is resolved in pekko-http
    */
  def xssHarden(underlying: server.Route): server.Route =
    respondWithHeader(
      RawHeader(
        "Content-Security-Policy", {
          val self = "'self'"
          val none = "'none'"
          val inline = "'unsafe-inline'"
          val eval = "'unsafe-eval'"

//        val any = "'*'"
          val anyDataBlob = "data:"
//        val anyHttp = "http:"
//        val anyHttps = "https:"
          val anyWs = "ws:"
          val anyWss = "wss:"

          val Csp = Map(
            "default-src" -> Vector(self), // in general, allow resources when they match the same origin policy
            "script-src" -> Vector( // only allow scripts that match the same origin policy... and allow eval() for plotly and vis-network
              self,
              eval,
            ),
            "object-src" -> Vector(none), // don't allow <object>, <embed>, or <applet>
            "style-src" -> Vector(self, inline), // allow scripts that match same origin or are provided inline
            "img-src" -> Vector( // allow images that match same origin or are provided as data: blobs
              self,
              anyDataBlob,
            ),
            "media-src" -> Vector(none), // don't allow <video>, <audio>, <source>, or <track>
            "frame-src" -> Vector(none), // don't allow <frame> or <iframe> on this page
            "font-src" -> Vector(self), // allow fonts that match same origin
            "connect-src" -> Vector( // allow HTTP requests to be sent by other (allowed) resources only if the destinations of those requests match the same origin policy
              self,
              anyWs, // NB this is way more permissive than we want. this allows connection to arbitrary websockets APIs, not just our own.
              anyWss, // However, connect-src 'self' doesn't include websockets on some browsers. See https://github.com/w3c/webappsec-csp/issues/7
            ),
          )

          Csp.toSeq.map { case (k, vs) => (k + vs.mkString(" ", " ", "")) }.mkString("; ")
        },
      ),
    )(underlying)

  /** Flow that will timeout after some fixed duration, provided that duration
    * is finite and the boolean override is not set
    *
    * @param dur how long after materialization to time out?
    * @param allowTimeout additional check that can be used to prevent/allow any timeout
    * @return flow that times out
    */
  def completionTimeoutOpt[A](dur: Duration, allowTimeout: Boolean = true): Flow[A, A, NotUsed] =
    dur match {
      case finite: FiniteDuration if allowTimeout => Flow[A].completionTimeout(finite)
      case _ => Flow[A]
    }
}

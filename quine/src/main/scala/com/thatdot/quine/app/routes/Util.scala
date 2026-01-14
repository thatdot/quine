package com.thatdot.quine.app.routes

import scala.annotation.unused
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.HttpHeader
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

  /** Constant values for use in Content Security Policy (CSP) headers. Abstracted to mitigate the
    * risk of introducing a security issue due to a silly typo.
    */
  private case object CspConstants {
    val self = "'self'"
    val none = "'none'"
    val inline = "'unsafe-inline'"
    val eval = "'unsafe-eval'"

    @unused val any = "'*'"
    val anyDataBlob = "data:"
    @unused val anyHttp = "http:"
    @unused val anyHttps = "https:"
  }

  /** Constants describing the frame embedding settings (to mitigate the risk of clickjacking attacks).
    * These should be kept in sync with one another.
    * When both X-Frame-Options and a CSP directive for `frame-ancestors` are set, modern browsers should,
    * per specification, prefer the CSP setting -- but older browsers may not have full CSP support.
    *
    * The current implementation encodes a same-origin embed policy -- that is, the UI pages may be embedded
    * only by a page served at the same domain, port, and protocol. This allows for embedding of the UI in
    * environments serving simple reverse proxies, without requiring the reverse proxy to manage manipulating
    * the CSP or X-Frames-Options headers.
    *
    * @see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Frame-Options
    * @see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Security-Policy/frame-ancestors
    * @see https://caniuse.com/mdn-http_headers_content-security-policy_frame-ancestors
    */
  private case object FrameEmbedSettings {
    import CspConstants._
    val legacyFrameOptionsHeader: HttpHeader =
      RawHeader(com.google.common.net.HttpHeaders.X_FRAME_OPTIONS, "SAMEORIGIN")
    val modernCspSetting: (String, Vector[String]) = "frame-ancestors" -> Vector(self)
  }

  /** Route-hardening operations, implicitly available via {{{import RouteHardeningOps.syntax._}}}.
    * <br/>
    * Consider improving these implementations if and when https://github.com/akka/akka-http/issues/155 ideas are
    * implemented in `pekko-http` (consider writing and offering the necessary changes to the library).
    */
  trait RouteHardeningOps {

    /** Harden the underlying route against XSS by providing a Content Security Policy
      *
      * @param underlying the route to protect
      * @return the augmented route
      * @see https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CSP
      */
    private def xssHarden(underlying: server.Route): server.Route =
      respondWithHeader(
        RawHeader(
          com.google.common.net.HttpHeaders.CONTENT_SECURITY_POLICY, {
            import CspConstants._

            val Csp = Map(
              "default-src" -> Vector(self), // in general, allow resources when they match the same origin policy
              "script-src" -> Vector(self), // only allow scripts that match the same origin policy
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
              ),
              FrameEmbedSettings.modernCspSetting,
            )

            Csp.toSeq.map { case (k, vs) => (k + vs.mkString(" ", " ", "")) }.mkString("; ")
          },
        ),
      )(underlying)

    /** Apply frame embedding hardening to prevent clickjacking attacks by setting X-Frame-Options header.
      * This adds the legacy `X-Frame-Options: SAMEORIGIN` header for older browsers that don't fully support
      * the modern CSP frame-ancestors directive.
      *
      * @param underlying the route to protect
      * @return the augmented route
      * @see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Frame-Options
      */
    private def frameEmbedHarden(underlying: server.Route): server.Route =
      respondWithHeader(FrameEmbedSettings.legacyFrameOptionsHeader)(underlying)

    /** Harden the underlying route with HTTP Strict Transport Security (HSTS) to enforce HTTPS connections
      * and prevent protocol downgrade attacks.
      *
      * @param underlying the route to protect
      * @return the augmented route
      * @see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Strict-Transport-Security
      */
    private def hstsHarden(underlying: server.Route): server.Route =
      respondWithHeader(
        RawHeader(
          com.google.common.net.HttpHeaders.STRICT_TRANSPORT_SECURITY,
          // 63,072,000 seconds is 2 years, longer than the minimum 1 year when including "preload". "preload" is
          // considered to be a request for inclusion in preloaded lists of HTTPS only domains found in web browsers.
          "max-age=63072000; includeSubDomains; preload",
        ),
      )(underlying)

    implicit class WithHardening(route: server.Route) {
      def withXssHardening: server.Route = xssHarden(route)
      def withFrameEmbedHardening: server.Route = frameEmbedHarden(route)
      def withHstsHardening: server.Route = hstsHarden(route)
      def withSecurityHardening: server.Route = hstsHarden(frameEmbedHarden(xssHarden(route)))
    }
  }

  object RouteHardeningOps {
    object syntax extends RouteHardeningOps
  }

  /** Flow that will time out after some fixed duration, provided that duration
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

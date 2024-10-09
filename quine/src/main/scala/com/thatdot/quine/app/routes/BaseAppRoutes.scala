package com.thatdot.quine.app.routes

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import org.apache.pekko.http.scaladsl.model.headers._
import org.apache.pekko.http.scaladsl.model.{HttpCharsets, HttpEntity, MediaType, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import nl.altindag.ssl.SSLFactory

import com.thatdot.quine.app.config.WebServerBindConfig
import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Tls.SSLFactoryBuilderOps

object MediaTypes {
  val `application/yaml` = MediaType.applicationWithFixedCharset("yaml", HttpCharsets.`UTF-8`, "yaml")
}

trait BaseAppRoutes extends LazySafeLogging with endpoints4s.pekkohttp.server.Endpoints {

  val graph: BaseGraph

  val timeout: Timeout

  implicit def idProvider: QuineIdProvider = graph.idProvider
  implicit lazy val materializer: Materializer = graph.materializer

  /** Serves up the static assets from resources and for JS/CSS dependencies */
  def staticFilesRoute: Route

  /** OpenAPI route */
  def openApiRoute: Route

  /** Rest API route */
  def apiRoute: Route

  /** Final HTTP route */
  def mainRoute: Route =
    Util.frameEmbedHarden(Util.xssHarden(staticFilesRoute)) ~
    redirectToNoTrailingSlashIfPresent(StatusCodes.PermanentRedirect) {
      apiRoute ~
      respondWithHeader(`Access-Control-Allow-Origin`.*) {
        // NB the following resources will be available to request from ANY source (including evilsite.com):
        // be sure this is what you want!
        openApiRoute
      }
    }

  /** Bind a webserver to server up the main route */
  def bindWebServer(interface: String, port: Int, useTls: Boolean): Future[Http.ServerBinding] = {
    import graph.system
    val serverBuilder = Http()(system)
      .newServerAt(interface, port)
      .adaptSettings(
        // See https://pekko.apache.org/docs/pekko-http/current//common/http-model.html#registering-custom-media-types
        _.mapWebsocketSettings(_.withPeriodicKeepAliveMaxIdle(10.seconds))
          .mapParserSettings(_.withCustomMediaTypes(MediaTypes.`application/yaml`)),
      )

    //capture unknown addresses with a 404
    val routeWithDefault =
      mainRoute ~ complete(StatusCodes.NotFound, HttpEntity("The requested resource could not be found."))

    val sslFactory: Option[SSLFactory] = Option.when(useTls) {
      val keystoreOverride =
        (sys.env.get(WebServerBindConfig.KeystorePathEnvVar) -> sys.env.get(
          WebServerBindConfig.KeystorePasswordEnvVar,
        )) match {
          case (Some(keystorePath), Some(password)) => Some(keystorePath -> password.toCharArray)
          case (Some(_), None) =>
            logger.warn(
              safe"""'${Safe(WebServerBindConfig.KeystorePathEnvVar)}' was specified but
                    |'${Safe(WebServerBindConfig.KeystorePasswordEnvVar)}' was not. Ignoring.
                    |""".cleanLines,
            )
            None
          case (None, Some(_)) =>
            logger.warn(
              safe"""'${Safe(WebServerBindConfig.KeystorePasswordEnvVar)}' was specified but
                    |'${Safe(WebServerBindConfig.KeystorePathEnvVar)}' was not. Ignoring.
                    |""".cleanLines,
            )
            None
          case (None, None) => None
        }
      val baseBuilder = SSLFactory
        .builder()
        .withSystemPropertyDerivedIdentityMaterial()
        .withSystemPropertyDerivedCiphersSafe()
        .withSystemPropertyDerivedProtocolsSafe()
      val builderWithOverride = keystoreOverride.fold(baseBuilder) { case (file, password) =>
        baseBuilder.withIdentityMaterial(file, password)
      }
      builderWithOverride.build()
    }

    sslFactory
      .fold(serverBuilder)(sslFactory =>
        serverBuilder.enableHttps(ConnectionContext.httpsServer(sslFactory.getSslContext)),
      )
      .bind(Route.toFunction(routeWithDefault)(system))
  }
}

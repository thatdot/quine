package com.thatdot.quine.app.routes

import java.nio.file.Paths

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.io.Source

import org.apache.pekko.http.scaladsl.model.headers._
import org.apache.pekko.http.scaladsl.model.{HttpCharsets, HttpEntity, MediaType, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import nl.altindag.ssl.SSLFactory

import com.thatdot.common.logging.Log.{LazySafeLogging, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.app.config.{UseMtls, WebServerBindConfig}
import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.Tls.SSLFactoryBuilderOps

object MediaTypes {
  val `application/yaml`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("yaml", HttpCharsets.`UTF-8`, "yaml")
}

trait BaseAppRoutes extends LazySafeLogging with endpoints4s.pekkohttp.server.Endpoints {

  val graph: BaseGraph

  val timeout: Timeout

  implicit def idProvider: QuineIdProvider = graph.idProvider
  implicit lazy val materializer: Materializer = graph.materializer

  /** Inject config values into JS resource and return as HttpEntity
    *
    * @param resourcePath path to the JS resource file
    * @param defaultV2Api whether to default to V2 API (true) or V1 API (false)
    * @return Route that serves the JS with injected config
    */
  protected def getJsWithInjectedConfig(resourcePath: String, defaultV2Api: Boolean): Route = {
    val resourceUrl = Option(getClass.getClassLoader.getResource(resourcePath))
    resourceUrl match {
      case Some(url) =>
        val source = Source.fromURL(url)
        try {
          val content = source.mkString
          val injectedContent = content.replace("/*{{DEFAULT_V2_API}}*/true", defaultV2Api.toString)
          val jsContentType = MediaType.applicationWithFixedCharset("javascript", HttpCharsets.`UTF-8`)
          complete(HttpEntity(jsContentType, injectedContent))
        } finally source.close()
      case None =>
        complete(StatusCodes.NotFound, s"Resource not found: $resourcePath")
    }
  }

  /** Serves up the static assets from resources and for JS/CSS dependencies */
  def staticFilesRoute: Route

  /** OpenAPI route */
  def openApiRoute: Route

  /** Rest API route */
  def apiRoute: Route

  /** Final HTTP route */
  def mainRoute: Route = {
    import Util.RouteHardeningOps.syntax._
    staticFilesRoute.withSecurityHardening ~
    redirectToNoTrailingSlashIfPresent(StatusCodes.PermanentRedirect) {
      apiRoute.withHstsHardening ~
      respondWithHeader(`Access-Control-Allow-Origin`.*) {
        // NB the following resources will be available to request from ANY source (including evilsite.com):
        // be sure this is what you want!
        openApiRoute.withSecurityHardening
      }
    }
  }

  /** Bind a webserver to server up the main route */
  def bindWebServer(
    interface: String,
    port: Int,
    useTls: Boolean,
    useMTls: UseMtls = UseMtls(),
  ): Future[Http.ServerBinding] = {
    import graph.system
    val serverBuilder = Http()(system)
      .newServerAt(interface, port)
      .adaptSettings(
        // See https://pekko.apache.org/docs/pekko-http/current//common/http-model.html#registering-custom-media-types
        _.mapWebsocketSettings(_.withPeriodicKeepAliveMaxIdle(10.seconds))
          .mapParserSettings(_.withCustomMediaTypes(MediaTypes.`application/yaml`)),
      )

    import Util.RouteHardeningOps.syntax._

    //capture unknown addresses with a 404
    val routeWithDefault =
      mainRoute ~ complete(
        StatusCodes.NotFound,
        HttpEntity("The requested resource could not be found."),
      ).withHstsHardening

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

      // Add truststore material for mTLS if enabled
      val builderWithTruststore = if (useMTls.enabled) {
        val truststoreOverride =
          // First priority: explicit truststore configuration
          useMTls.trustStore
            .map { mtlsTs =>
              mtlsTs.path.getAbsolutePath -> mtlsTs.password.toCharArray
            }
            .orElse {
              // Fallback: system properties
              (sys.props.get("javax.net.ssl.trustStore") -> sys.props.get("javax.net.ssl.trustStorePassword")) match {
                case (Some(truststorePath), Some(password)) => Some(truststorePath -> password.toCharArray)
                case (Some(_), None) =>
                  logger.warn(
                    safe"""'javax.net.ssl.trustStore' was specified but 'javax.net.ssl.trustStorePassword' was not.
                        |Client certificate validation will not work as expected.
                        |""".cleanLines,
                  )
                  None
                case (None, Some(_)) =>
                  logger.warn(
                    safe"""'javax.net.ssl.trustStorePassword' was specified but 'javax.net.ssl.trustStore' was not.
                        |Client certificate validation will not work as expected.
                        |""".cleanLines,
                  )
                  None
                case (None, None) =>
                  logger.warn(
                    safe"""mTLS is enabled but no truststore is configured. Neither 'useMtls.trustStore' was set
                        |nor were 'javax.net.ssl.trustStore' and 'javax.net.ssl.trustStorePassword' system properties.
                        |Client certificates will not be validated.
                        |""".cleanLines,
                  )
                  None
              }
            }
        truststoreOverride.fold(builderWithOverride) { case (filePath, password) =>
          builderWithOverride.withTrustMaterial(Paths.get(filePath), password)
        }
      } else {
        builderWithOverride
      }

      builderWithTruststore.build()
    }

    // Create connection context with mTLS support if enabled
    val connectionContext = sslFactory.map { factory =>
      if (useMTls.enabled) {
        ConnectionContext.httpsServer { () =>
          val engine = factory.getSslContext.createSSLEngine()
          engine.setUseClientMode(false)
          engine.setNeedClientAuth(true)
          engine
        }
      } else {
        ConnectionContext.httpsServer(factory.getSslContext)
      }
    }

    connectionContext
      .fold(serverBuilder)(serverBuilder.enableHttps(_))
      .bind(Route.toFunction(routeWithDefault)(system))
  }
}

package com.thatdot.quine.app.routes

import java.io.{File, FileInputStream}
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Using

import org.apache.pekko.http.scaladsl.model.headers._
import org.apache.pekko.http.scaladsl.model.{HttpCharsets, HttpEntity, MediaType, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import com.thatdot.quine.app.config.SslConfig
import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.Log._

object MediaTypes {
  val `application/yaml` = MediaType.applicationWithFixedCharset("yaml", HttpCharsets.`UTF-8`, "yaml")
}

object SslHelper {

  /** Create an SSL context given the path to a Java keystore and its password
    * @param path
    * @param password
    * @return
    */
  def sslContextFromKeystore(path: File, password: Array[Char]): SSLContext = {
    val keystore = KeyStore.getInstance(KeyStore.getDefaultType)
    Using.resource(new FileInputStream(path))(keystoreFile => keystore.load(keystoreFile, password))
    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagerFactory.init(keystore, password)
    val trustManagerFacotry = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFacotry.init(keystore)
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, trustManagerFacotry.getTrustManagers, new SecureRandom)
    sslContext
  }
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
    Util.xssHarden(staticFilesRoute) ~
    redirectToNoTrailingSlashIfPresent(StatusCodes.PermanentRedirect) {
      apiRoute ~
      respondWithHeader(`Access-Control-Allow-Origin`.*) {
        // NB the following resources will be available to request from ANY source (including evilsite.com):
        // be sure this is what you want!
        openApiRoute
      }
    }

  /** Bind a webserver to server up the main route */
  def bindWebServer(interface: String, port: Int, ssl: Option[SslConfig]): Future[Http.ServerBinding] = {
    import graph.system
    val serverBuilder = Http()(system)
      .newServerAt(interface, port)
      .adaptSettings(
        // See https://pekko.apache.org/docs/pekko-http/current//common/http-model.html#registering-custom-media-types
        _.mapWebsocketSettings(_.withPeriodicKeepAliveMaxIdle(10.seconds))
          .mapParserSettings(_.withCustomMediaTypes(MediaTypes.`application/yaml`))
      )

    //capture unknown addresses with a 404
    val routeWithDefault =
      mainRoute ~ complete(StatusCodes.NotFound, HttpEntity("The requested resource could not be found."))
    ssl
      .fold(serverBuilder) { ssl =>
        serverBuilder.enableHttps(
          ConnectionContext.httpsServer(SslHelper.sslContextFromKeystore(ssl.path, ssl.password))
        )
      }
      .bind(
        Route.toFunction(routeWithDefault)(system)
      )
  }
}

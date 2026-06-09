package com.thatdot.quine.app.routes

import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.pekko.http.scaladsl.model.{HttpCharsets, HttpEntity, MediaType, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route

import org.webjars.WebJarAssetLocator

/** Failure modes for [[BrowserBundleRoutes.resolveBundleFilename]]. */
sealed trait BundleResolveError {
  def message: String
}
object BundleResolveError {
  final case class NotFound(prefix: String) extends BundleResolveError {
    def message: String =
      s"Could not find content-hashed bundle matching '$prefix.<hash>.js' on the webjar " +
      s"classpath. Did the browser project's webpack pipeline run?"
  }
  final case class MultipleMatches(prefix: String, names: Set[String]) extends BundleResolveError {
    def message: String =
      s"Found multiple content-hashed bundles matching '$prefix.<hash>.js': " +
      s"${names.mkString(", ")}. Clean stale build artifacts and rebuild."
  }
}

/** Mix-in for routes that serve a webpack-bundled browser app. Adds discovery of the
  * content-hashed bundle filename and the helpers used to serve the HTML entry pages,
  * runtime-templated startup JS, and webjar-hosted static assets. Concrete classes supply only
  * the [[bundlePrefix]] string. Mixed in alongside [[BaseAppRoutes]] on routes that serve a UI;
  * health-probe-only routes skip this trait.
  */
trait BrowserBundleRoutes {

  /** The unhashed prefix of the browser bundle (e.g. "quine-browser-bundle"). The actual filename
    * on the classpath is `<prefix>.<hash>.js`, where the hash is set by webpack and changes per
    * build.
    */
  protected def bundlePrefix: String

  /** Classpath-backed locator for webjar assets (Scala.js bundle, third-party libs, fonts, etc.).
    * Constructed once because the underlying ClassGraph scan is not free.
    */
  protected val webJarAssetLocator = new WebJarAssetLocator()

  /** Content-hashed filename of the browser webpack bundle, discovered at startup by scanning
    * the webjar classpath for [[bundlePrefix]]. Throws at server boot if the bundle can't be
    * located — there is no UI to serve without it.
    */
  protected lazy val hashedBundleName: String =
    resolveBundleFilename(bundlePrefix).fold(err => throw new IllegalStateException(err.message), identity)

  /** Discover the content-hashed browser bundle filename by scanning the webjar classpath for a
    * single file matching `<bundlePrefix>.<hash>.js`. The webpack configs emit filenames like
    * `quine-browser-bundle.[contenthash].js`; the hash changes per build, so the server cannot
    * hard-code it.
    *
    * @param bundlePrefix the unhashed prefix (e.g. "quine-browser-bundle")
    * @return the actual filename on the classpath (e.g. "quine-browser-bundle.a1b2c3d4.js"), or
    *         a [[BundleResolveError]] variant describing why the lookup failed
    */
  protected def resolveBundleFilename(bundlePrefix: String): Either[BundleResolveError, String] = {
    val pattern = (java.util.regex.Pattern.quote(bundlePrefix) + """\.[0-9a-f]+\.js""").r.pattern
    val matches = webJarAssetLocator
      .listAssets("")
      .asScala
      .iterator
      .map(_.split('/').last)
      .filter(pattern.matcher(_).matches())
      .toSet
    matches.toSeq match {
      case Seq(single) => Right(single)
      case Seq() => Left(BundleResolveError.NotFound(bundlePrefix))
      case multiple => Left(BundleResolveError.MultipleMatches(bundlePrefix, multiple.toSet))
    }
  }

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

  /** Serve an HTML entry page, rewriting the unhashed `<script>` reference to the actual
    * content-hashed bundle filename discovered at startup. The on-disk HTML keeps the literal
    * unhashed name (`<prefix>.js`) so it still renders if opened directly; the server substitutes
    * it at request time.
    *
    * @param resourcePath        path to the HTML resource file (e.g. "web/quine-ui.html")
    * @param unhashedBundleName  the literal placeholder in the HTML (e.g. "quine-browser-bundle.js")
    * @param hashedBundleName    the actual filename to substitute (from [[resolveBundleFilename]])
    */
  protected def getHtmlWithInjectedBundleName(
    resourcePath: String,
    unhashedBundleName: String,
    hashedBundleName: String,
  ): Route = {
    val resourceUrl = Option(getClass.getClassLoader.getResource(resourcePath))
    resourceUrl match {
      case Some(url) =>
        val source = Source.fromURL(url)
        try {
          val content = source.mkString.replace(unhashedBundleName, hashedBundleName)
          val htmlContentType = MediaType.textWithFixedCharset("html", HttpCharsets.`UTF-8`)
          complete(HttpEntity(htmlContentType, content))
        } finally source.close()
      case None =>
        complete(StatusCodes.NotFound, s"Resource not found: $resourcePath")
    }
  }

  /** Catch-all route for webjar-hosted static assets (third-party JS/CSS, fonts, and the
    * content-hashed Scala.js bundle itself). Requests for unknown paths fall through here and are
    * resolved against the classpath via [[webJarAssetLocator]]; when the request matches the
    * passed-in `hashedBundleName`, the response is served with `Cache-Control: ..., immutable`.
    * Long-lived immutable caching is only safe for content-addressed (hashed) filenames; other
    * webjar assets keep their default heuristic-freshness behavior.
    */
  protected def webjarFallbackRoute(hashedBundleName: String): Route = {
    import Util.CacheControlOps.syntax._
    extractUnmatchedPath { path =>
      val pathStr = path.toString
      val baseRoute = Try(webJarAssetLocator.getFullPath(pathStr)) match {
        case Success(fullPath) => getFromResource(fullPath)
        case Failure(_: IllegalArgumentException) => reject
        case Failure(err) => failWith(err)
      }
      if (pathStr.split('/').lastOption.contains(hashedBundleName)) baseRoute.withImmutableCache
      else baseRoute
    }
  }
}

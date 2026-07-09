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

  /** The artifact id of this app's own webjar (e.g. "quine", "quine-enterprise", "novelty") —
    * the directory name under `META-INF/resources/webjars/` that this app's own webpack build
    * publishes to. Used to disambiguate un-hashed asset lookups (see [[webjarFallbackRoute]])
    * when the same filename is emitted by more than one webjar on the classpath, e.g.
    * `editor.worker.js`, which every Scala.js project with the Monaco query editor emits under
    * its own webjar. A dependent app (quine-enterprise-browser depends on quine-browser;
    * novelty-browser depends on both) always has more than one such webjar on its classpath, so
    * lookups by filename alone are ambiguous.
    */
  protected def webJarName: String

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
          val injectedContent = content
            .replace("/*{{DEFAULT_V2_API}}*/true", defaultV2Api.toString)
            .replace("/*{{QP_ENABLED}}*/false", QuinePatternSettings.isEnabled.toString)
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

  /** In-memory cache of classpath resources. Pre-reading avoids the ZipFile race condition
    * in Pekko HTTP's `getFromResource` that causes NPEs under heavy thread contention
    * (java.util.zip.ZipEntry null when concurrent threads read from the same jar).
    */
  private val resourceCache = new java.util.concurrent.ConcurrentHashMap[String, Array[Byte]]()

  private def cachedResourceBytes(fullPath: String): Option[Array[Byte]] = {
    val cached = resourceCache.get(fullPath)
    if (cached != null) Some(cached)
    else {
      val stream = getClass.getClassLoader.getResourceAsStream(fullPath)
      if (stream == null) None
      else {
        val bytes =
          try stream.readAllBytes()
          finally stream.close()
        resourceCache.putIfAbsent(fullPath, bytes)
        Some(bytes)
      }
    }
  }

  /** Drop-in replacement for Pekko HTTP's `getFromResource` that reads from an in-memory cache.
    * Use this instead of `getFromResource` to avoid the ZipFile NPE under heavy load.
    */
  protected def cachedGetFromResource(resourcePath: String): Route =
    cachedResourceBytes(resourcePath) match {
      case Some(bytes) =>
        import org.apache.pekko.http.scaladsl.model._
        val ext = resourcePath.split('.').lastOption.getOrElse("")
        val ct = MediaTypes.forExtension(ext) match {
          case binary: MediaType.Binary => ContentType(binary)
          case wfc: MediaType.WithFixedCharset => ContentType(wfc)
          case woc: MediaType.WithOpenCharset => ContentType.WithCharset(woc, HttpCharsets.`UTF-8`)
        }
        complete(HttpEntity(ct, bytes))
      case None => reject
    }

  /** Resolve a webjar-relative asset path, preferring the asset published by this app's own
    * webjar ([[webJarName]]) when more than one webjar on the classpath has a file by that name
    * (e.g. `editor.worker.js`, emitted by every Scala.js project with the Monaco query editor).
    * Falls back to an unscoped lookup for assets that only ever live in one webjar (third-party
    * libs, fonts, the content-hashed bundle itself).
    */
  private def resolveAssetPath(pathStr: String): Try[String] =
    Try(webJarAssetLocator.getFullPath(webJarName, pathStr)).recoverWith { case _: IllegalArgumentException =>
      Try(webJarAssetLocator.getFullPath(pathStr))
    }

  /** Catch-all route for webjar-hosted static assets (third-party JS/CSS, fonts, and the
    * content-hashed Scala.js bundle itself). Resources are read from the classpath into memory
    * on first access and served from cache thereafter, avoiding Pekko HTTP's `getFromResource`
    * which is susceptible to ZipFile race conditions under heavy load.
    */
  protected def webjarFallbackRoute(hashedBundleName: String): Route = {
    import Util.CacheControlOps.syntax._
    extractUnmatchedPath { path =>
      val pathStr = path.toString
      val baseRoute = resolveAssetPath(pathStr) match {
        case Success(fullPath) =>
          cachedResourceBytes(fullPath) match {
            case Some(bytes) =>
              import org.apache.pekko.http.scaladsl.model._
              val ext = fullPath.split('.').lastOption.getOrElse("")
              val ct = MediaTypes.forExtension(ext) match {
                case binary: MediaType.Binary => ContentType(binary)
                case wfc: MediaType.WithFixedCharset => ContentType(wfc)
                case woc: MediaType.WithOpenCharset => ContentType.WithCharset(woc, HttpCharsets.`UTF-8`)
              }
              complete(HttpEntity(ct, bytes))
            case None => reject
          }
        case Failure(_: IllegalArgumentException) => reject
        case Failure(err) => failWith(err)
      }
      if (pathStr.split('/').lastOption.contains(hashedBundleName)) baseRoute.withImmutableCache
      else baseRoute
    }
  }
}

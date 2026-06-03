package com.thatdot.quine.app.model.ingest2.sources

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken, RawHeader}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Framing, Sink}
import org.apache.pekko.util.ByteString

import io.circe.{Json, JsonObject}

import com.thatdot.common.logging.Log._
import com.thatdot.common.security.Secret
import com.thatdot.common.security.Secret.Unsafe._
import com.thatdot.quine.app.model.ingest2.{
  BearerTokenAuth,
  DeltaSharingAuth,
  OAuthCertificateAuth,
  OAuthClientCredentials,
}

// ── Data model classes for Delta Sharing protocol responses ──

/** Protocol information from the Delta Sharing server. */
case class DeltaSharingProtocol(minReaderVersion: Int)

/** Table metadata from the Delta Sharing server. */
case class DeltaSharingMetadata(
  id: String,
  schemaString: Option[String],
  configuration: Map[String, String],
)

/** A file action (add, cdc, cdf, or file) from a Delta Sharing changes or query response. */
case class DeltaSharingFileAction(
  actionType: String, // "add", "cdc", "cdf", or "file"
  url: String,
  size: Long,
  partitionValues: Map[String, String],
  version: Long,
  timestamp: Long,
  // Delta-format fields for deletion vector support
  deletionVectorFileUrl: Option[String] = None,
  deletionVectorSizeInBytes: Option[Long] = None,
  deletionVectorStorageType: Option[String] = None,
  deletionVectorOffset: Option[Long] = None,
  deletionVectorCardinality: Option[Long] = None,
)

/** Response from queryTableChanges or queryTableSnapshot. */
case class DeltaSharingChangesResponse(
  protocol: Option[DeltaSharingProtocol],
  metadata: Option[DeltaSharingMetadata],
  files: Seq[DeltaSharingFileAction],
  // Raw JSON lines for delta log construction (populated when responseFormat=delta)
  rawProtocolJson: Option[String] = None,
  rawMetadataJson: Option[String] = None,
  rawActionJsons: Seq[String] = Seq.empty,
)

/** Whether a file action has an associated deletion vector that requires Delta Kernel to read. */
object DeltaSharingFileAction {
  def hasDeletionVector(action: DeltaSharingFileAction): Boolean =
    action.deletionVectorFileUrl.isDefined
}

/** Response from queryTableMetadata (startup validation). */
case class DeltaSharingMetadataResponse(
  protocol: DeltaSharingProtocol,
  metadata: DeltaSharingMetadata,
)

// ── OAuth token management ──

/** Cached OAuth token with expiry time. */
private case class CachedToken(accessToken: String, expiresAt: Long)

/** Manages OAuth Client Credentials token acquisition and refresh. */
private class OAuthTokenManager(
  tokenEndpoint: String,
  clientId: String,
  clientSecret: Secret,
  scope: Option[String],
)(implicit system: ActorSystem, mat: Materializer)
    extends LazySafeLogging {

  implicit private val ec: ExecutionContext = system.dispatcher

  private val cachedToken: AtomicReference[Option[CachedToken]] = new AtomicReference(None)

  /** Refresh margin: refresh the token when it's within this many seconds of expiry. */
  private val RefreshMarginSeconds = 60L

  /** Get a valid bearer token, refreshing if necessary. */
  def getToken(): Future[String] = {
    val cached = cachedToken.get()
    cached match {
      case Some(token) if !isNearExpiry(token) =>
        Future.successful(token.accessToken)
      case _ =>
        refreshToken()
    }
  }

  /** Force a token refresh (e.g., on 401). */
  def refreshToken(): Future[String] = {
    val formData = FormData(
      Map(
        "grant_type" -> "client_credentials",
        "client_id" -> clientId,
        "client_secret" -> clientSecret.unsafeValue,
      ) ++ scope.map("scope" -> _),
    )

    Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = Uri(tokenEndpoint),
          entity = formData.toEntity,
        ),
      )
      .flatMap { response =>
        response.entity.toStrict(30.seconds).map { entity =>
          val body = entity.data.utf8String
          val json = io.circe.parser.parse(body).getOrElse(Json.Null)
          val cursor = json.hcursor

          response.status match {
            case StatusCodes.OK =>
              val accessToken = cursor
                .get[String]("access_token")
                .getOrElse(
                  throw new DeltaSharingException("OAuth token response missing 'access_token' field"),
                )
              val expiresIn = cursor.get[Long]("expires_in").getOrElse(3600L)
              val expiresAt = System.currentTimeMillis() / 1000 + expiresIn
              val token = CachedToken(accessToken, expiresAt)
              cachedToken.set(Some(token))
              accessToken
            case _ =>
              val errorDesc = cursor.get[String]("error_description").getOrElse(body)
              throw new DeltaSharingException(
                s"OAuth token request to $tokenEndpoint failed with status ${response.status}: $errorDesc",
              )
          }
        }
      }
  }

  private def isNearExpiry(token: CachedToken): Boolean =
    System.currentTimeMillis() / 1000 >= token.expiresAt - RefreshMarginSeconds
}

// ── Exceptions ──

/** Exception type for Delta Sharing client errors with actionable messages. */
class DeltaSharingException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

// ── Delta Sharing HTTP client ──

/** HTTP client for the Delta Sharing open protocol.
  *
  * Supports both bearer token (credential file v1) and OAuth Client Credentials
  * (credential file v2 / OIDC federation) authentication.
  */
class DeltaSharingClient(
  endpoint: String,
  auth: DeltaSharingAuth,
  serverTimeoutMs: Long,
  parquetFetchTimeoutMs: Long,
  maxRetries: Int,
  requestDeltaFormat: Boolean = false,
  certTokenAcquirer: Option[OAuthCertificateAuth => Future[String]] = None,
)(implicit system: ActorSystem)
    extends LazySafeLogging {

  implicit private val ec: ExecutionContext = system.dispatcher
  implicit private val mat: Materializer = Materializer(system)

  private val serverTimeout: FiniteDuration = serverTimeoutMs.millis
  private val parquetTimeout: FiniteDuration = parquetFetchTimeoutMs.millis

  /** Strip trailing slash from endpoint for clean URL construction. */
  private val baseUrl: String = endpoint.stripSuffix("/")

  private val oauthManager: Option[OAuthTokenManager] = auth match {
    case OAuthClientCredentials(tokenEndpoint, clientId, clientSecret, scope) =>
      Some(new OAuthTokenManager(tokenEndpoint, clientId, clientSecret, scope))
    case _: BearerTokenAuth => None
    case _: OAuthCertificateAuth => None
  }

  /** Get the current bearer token, refreshing OAuth tokens as needed. */
  private def currentBearerToken(): Future[String] = auth match {
    case BearerTokenAuth(token) =>
      Future.successful(token.unsafeValue)
    case _: OAuthClientCredentials =>
      oauthManager.get.getToken()
    case cert: OAuthCertificateAuth =>
      certTokenAcquirer match {
        case Some(acquirer) => acquirer(cert)
        case None =>
          Future.failed(
            new DeltaSharingException(
              "Certificate authentication (OAuthCertificateAuth) requires Quine Enterprise. " +
              "Use BearerToken or OAuthClientCredentials auth instead.",
            ),
          )
      }
  }

  /** Delta Sharing capability header for requesting delta format responses. */
  private val deltaCapabilityHeader = RawHeader(
    "delta-sharing-capabilities",
    "responseformat=delta;readerfeatures=deletionvectors,columnmapping",
  )

  /** Build an authorized HTTP request, optionally with delta format capability header. */
  private def authorizedRequest(method: HttpMethod, uri: String): Future[HttpRequest] =
    currentBearerToken().map { token =>
      val headers = List(Authorization(OAuth2BearerToken(token))) ++
        (if (requestDeltaFormat) List(deltaCapabilityHeader) else Nil)
      logger.info(
        safe"Databricks Delta Sharing request: ${Safe(method.value)} ${Safe(uri)} " +
        safe"deltaFormatCapability=${Safe(if (requestDeltaFormat) deltaCapabilityHeader.value else "(none)")}",
      )
      HttpRequest(
        method = method,
        uri = Uri(uri),
        headers = headers,
      )
    }

  /** Execute an HTTP request with retry logic and error handling.
    *
    * Retries on 5xx and 429. On 401, attempts one OAuth token refresh (for OAuth auth)
    * or fails immediately (for bearer token auth). Fails fast on 403, 404.
    */
  private def executeWithRetry(
    buildRequest: () => Future[HttpRequest],
    timeout: FiniteDuration,
    retriesLeft: Int = maxRetries,
    is401Retry: Boolean = false,
  ): Future[HttpResponse] =
    buildRequest().flatMap { request =>
      val requestUri = request.uri.toString
      Http().singleRequest(request).flatMap { response =>
        if (response.status.intValue >= 400) {
          logger.warn(
            safe"Databricks Delta Sharing response: status=${Safe(response.status.toString)} " +
            safe"uri=${Safe(requestUri)}",
          )
        }
        response.status match {
          case StatusCodes.OK | StatusCodes.Created =>
            Future.successful(response)

          case StatusCodes.Unauthorized =>
            // Discard the response entity to free the connection
            response.entity.discardBytes()
            auth match {
              case _: OAuthClientCredentials if !is401Retry =>
                // Refresh token and retry once
                oauthManager.get.refreshToken().flatMap { _ =>
                  executeWithRetry(buildRequest, timeout, retriesLeft, is401Retry = true)
                }
              case _: OAuthClientCredentials =>
                Future.failed(
                  new DeltaSharingException(
                    "Delta Sharing request failed with 401 after OAuth token refresh. " +
                    "Check that the client credentials and OIDC federation configuration are correct.",
                  ),
                )
              case _: BearerTokenAuth =>
                Future.failed(
                  new DeltaSharingException(
                    "Delta Sharing bearer token is invalid or expired. " +
                    "Rotate credentials and restart the ingest.",
                  ),
                )
              case _: OAuthCertificateAuth =>
                Future.failed(
                  new DeltaSharingException(
                    "Delta Sharing certificate authentication failed with 401. " +
                    "Check the certificate, client ID, and IdP configuration.",
                  ),
                )
            }

          case StatusCodes.Forbidden =>
            response.entity.toStrict(timeout).flatMap { entity =>
              val body = entity.data.utf8String
              Future.failed(
                new DeltaSharingException(
                  s"Delta Sharing request forbidden (403). " +
                  s"Check that the recipient has SELECT access on the share. Response: $body",
                ),
              )
            }

          case StatusCodes.NotFound =>
            response.entity.toStrict(timeout).flatMap { entity =>
              val body = entity.data.utf8String
              Future.failed(
                new DeltaSharingException(
                  s"Delta Sharing resource not found (404). " +
                  s"Verify the share, schema, and table names. Response: $body",
                ),
              )
            }

          case StatusCodes.TooManyRequests if retriesLeft > 0 =>
            response.entity.discardBytes()
            val retryAfter = response.headers
              .find(_.lowercaseName == "retry-after")
              .flatMap(h => scala.util.Try(h.value.toLong).toOption)
              .getOrElse(10L)
            org.apache.pekko.pattern
              .after(retryAfter.seconds, system.scheduler)(
                executeWithRetry(buildRequest, timeout, retriesLeft - 1, is401Retry),
              )

          case status if status.intValue >= 500 && retriesLeft > 0 =>
            response.entity.discardBytes()
            val backoff = backoffDelay(maxRetries - retriesLeft)
            org.apache.pekko.pattern
              .after(backoff, system.scheduler)(
                executeWithRetry(buildRequest, timeout, retriesLeft - 1, is401Retry),
              )

          case StatusCodes.BadRequest =>
            response.entity.toStrict(timeout).flatMap { entity =>
              val body = entity.data.utf8String
              Future.failed(
                new DeltaSharingException(
                  s"Delta Sharing bad request (400): $body",
                ),
              )
            }

          case status =>
            response.entity.toStrict(timeout).flatMap { entity =>
              val body = entity.data.utf8String
              Future.failed(
                new DeltaSharingException(
                  s"Delta Sharing request failed with status $status: $body",
                ),
              )
            }
        }
      }
    }

  /** Exponential backoff with jitter. */
  private def backoffDelay(attempt: Int): FiniteDuration = {
    val baseMs = math.min(1000L * math.pow(2.0, attempt.toDouble).toLong, 60000L)
    val jitter = (Random.nextDouble() * baseMs * 0.2).toLong
    (baseMs + jitter).millis
  }

  /** Lightweight version check. Returns the current Delta table version.
    *
    * Tries the dedicated `/version` endpoint first. If the server returns 404
    * (some servers, including the OSS reference server, do not implement this
    * endpoint), falls back to reading the `Delta-Table-Version` header from
    * the `/metadata` response.
    */
  def queryTableVersion(share: String, schema: String, table: String): Future[Long] = {
    val url = s"$baseUrl/shares/$share/schemas/$schema/tables/$table/version"
    authorizedRequest(HttpMethods.GET, url)
      .flatMap(Http().singleRequest(_))
      .flatMap { response =>
        response.status match {
          case StatusCodes.OK =>
            val version = extractVersionHeader(response)
            response.entity.discardBytes()
            Future.successful(version)
          case StatusCodes.NotFound =>
            // Fallback: get version from metadata endpoint header
            response.entity.discardBytes()
            queryTableVersionFromMetadata(share, schema, table)
          case _ =>
            // For other errors, go through the normal retry/error handling
            response.entity.discardBytes()
            executeWithRetry(() => authorizedRequest(HttpMethods.GET, url), serverTimeout).flatMap { r =>
              val version = extractVersionHeader(r)
              r.entity.discardBytes()
              Future.successful(version)
            }
        }
      }
  }

  /** Extract Delta-Table-Version header from a response, or throw. */
  private def extractVersionHeader(response: HttpResponse): Long =
    response.headers
      .find(_.lowercaseName == "delta-table-version")
      .map(_.value.toLong)
      .getOrElse(
        throw new DeltaSharingException(
          "Delta Sharing response missing Delta-Table-Version header",
        ),
      )

  /** Fallback: get version from the metadata endpoint's response header. */
  private def queryTableVersionFromMetadata(share: String, schema: String, table: String): Future[Long] = {
    val url = s"$baseUrl/shares/$share/schemas/$schema/tables/$table/metadata"
    executeWithRetry(() => authorizedRequest(HttpMethods.GET, url), serverTimeout).flatMap { response =>
      val version = extractVersionHeader(response)
      response.entity.discardBytes()
      Future.successful(version)
    }
  }

  /** Fetch table metadata for startup validation. */
  def queryTableMetadata(share: String, schema: String, table: String): Future[DeltaSharingMetadataResponse] = {
    val url = s"$baseUrl/shares/$share/schemas/$schema/tables/$table/metadata"
    executeWithRetry(() => authorizedRequest(HttpMethods.GET, url), serverTimeout).flatMap { response =>
      parseNdjsonResponse(response).map { parsed =>
        DeltaSharingMetadataResponse(
          protocol = parsed.protocol.getOrElse(
            throw new DeltaSharingException("Metadata response missing protocol line"),
          ),
          metadata = parsed.metadata.getOrElse(
            throw new DeltaSharingException("Metadata response missing metadata line"),
          ),
        )
      }
    }
  }

  /** Fetch CDF change events for the given version range. */
  def queryTableChanges(
    share: String,
    schema: String,
    table: String,
    startingVersion: Long,
    endingVersion: Option[Long] = None,
  ): Future[DeltaSharingChangesResponse] = {
    val endParam = endingVersion.map(v => s"&endingVersion=$v").getOrElse("")
    val url = s"$baseUrl/shares/$share/schemas/$schema/tables/$table/changes?startingVersion=$startingVersion$endParam"
    executeWithRetry(() => authorizedRequest(HttpMethods.GET, url), serverTimeout).flatMap(parseNdjsonResponse)
  }

  /** Fetch a full table snapshot (for snapshotOnFirstRun). */
  def queryTableSnapshot(share: String, schema: String, table: String): Future[DeltaSharingChangesResponse] = {
    val url = s"$baseUrl/shares/$share/schemas/$schema/tables/$table/query"
    executeWithRetry(
      () =>
        authorizedRequest(HttpMethods.POST, url).map(
          _.withEntity(HttpEntity(ContentTypes.`application/json`, "{}")),
        ),
      serverTimeout,
    ).flatMap(parseNdjsonResponse)
  }

  /** Fetch a Parquet file from a presigned cloud storage URL.
    * No auth header needed — the URL contains the auth in its query parameters.
    * Retries on transient errors (5xx, network failures) up to `maxRetries` times.
    */
  def fetchParquetFile(presignedUrl: String, retriesLeft: Int = maxRetries): Future[Array[Byte]] = {
    // Log host+path but NOT the query string — presigned URLs carry the signature there.
    val parsedUri = Uri(presignedUrl)
    val safeHost = parsedUri.authority.host.toString
    val safePath = parsedUri.path.toString
    logger.info(
      safe"Delta Sharing parquet fetch: host=${Safe(safeHost)} path=${Safe(safePath)} " +
      safe"hasQuery=${Safe(parsedUri.rawQueryString.isDefined.toString)} " +
      safe"attemptsLeft=${Safe(retriesLeft.toString)}",
    )
    Http()
      .singleRequest(HttpRequest(uri = parsedUri))
      .flatMap { response =>
        response.status match {
          case StatusCodes.OK =>
            response.entity.dataBytes
              .runFold(ByteString.empty)(_ ++ _)
              .map { bytes =>
                logger.info(
                  safe"Delta Sharing parquet response: 200 host=${Safe(safeHost)} " +
                  safe"path=${Safe(safePath)} bytes=${Safe(bytes.length.toString)}",
                )
                bytes.toArrayUnsafe()
              }
          case status if status.intValue >= 500 && retriesLeft > 0 =>
            logger.warn(
              safe"Delta Sharing parquet response: ${Safe(status.toString)} host=${Safe(safeHost)} " +
              safe"path=${Safe(safePath)} (retrying)",
            )
            response.entity.discardBytes()
            val backoff = backoffDelay(maxRetries - retriesLeft)
            org.apache.pekko.pattern
              .after(backoff, system.scheduler)(
                fetchParquetFile(presignedUrl, retriesLeft - 1),
              )
          case status =>
            logger.warn(
              safe"Delta Sharing parquet response: ${Safe(status.toString)} host=${Safe(safeHost)} " +
              safe"path=${Safe(safePath)} (terminal)",
            )
            response.entity.toStrict(parquetTimeout).flatMap { entity =>
              Future.failed(
                new DeltaSharingException(
                  s"Failed to fetch Parquet file (HTTP $status). " +
                  s"The presigned URL may have expired. Response: ${entity.data.utf8String.take(500)}",
                ),
              )
            }
        }
      }
      .recoverWith {
        case _: java.net.ConnectException if retriesLeft > 0 =>
          val backoff = backoffDelay(maxRetries - retriesLeft)
          org.apache.pekko.pattern.after(backoff, system.scheduler)(
            fetchParquetFile(presignedUrl, retriesLeft - 1),
          )
      }
  }

  // ── NDJSON response parsing ──

  /** Parse an NDJSON response body into a DeltaSharingChangesResponse.
    *
    * Each line is a JSON object with exactly one top-level key indicating its type:
    * "protocol", "metaData", "add", "cdc", or "remove".
    */
  private def parseNdjsonResponse(response: HttpResponse): Future[DeltaSharingChangesResponse] =
    response.entity.dataBytes
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 10 * 1024 * 1024, allowTruncation = true))
      .map(_.utf8String.trim)
      .filter(_.nonEmpty)
      .runWith(Sink.seq)
      .map { lines =>
        var protocol: Option[DeltaSharingProtocol] = None
        var metadata: Option[DeltaSharingMetadata] = None
        val files = Seq.newBuilder[DeltaSharingFileAction]
        // For delta format: preserve raw JSON lines for delta log construction
        var rawProtocol: Option[String] = None
        var rawMetadata: Option[String] = None
        val rawActions = Seq.newBuilder[String]

        lines.foreach { line =>
          io.circe.parser.parse(line) match {
            case Right(json) =>
              val obj = json.asObject.getOrElse(JsonObject.empty)
              if (obj.contains("protocol")) {
                val p = json.hcursor.downField("protocol")
                // Delta format nests under "deltaProtocol"; parquet format is flat
                val minReader = p
                  .get[Int]("minReaderVersion")
                  .orElse(p.downField("deltaProtocol").get[Int]("minReaderVersion"))
                  .getOrElse(1)
                protocol = Some(DeltaSharingProtocol(minReaderVersion = minReader))
                rawProtocol = Some(line)
              } else if (obj.contains("metaData")) {
                val m = json.hcursor.downField("metaData")
                // Delta format nests under "deltaMetadata"; parquet format is flat
                val dm = m.downField("deltaMetadata")
                val isDeltaFormat = dm.succeeded
                val metaSource = if (isDeltaFormat) dm else m
                metadata = Some(
                  DeltaSharingMetadata(
                    id = metaSource.get[String]("id").getOrElse(""),
                    schemaString = metaSource.get[String]("schemaString").toOption,
                    configuration = metaSource
                      .downField("configuration")
                      .as[Map[String, String]]
                      .getOrElse(Map.empty),
                  ),
                )
                rawMetadata = Some(line)
              } else if (obj.contains("add") || obj.contains("cdc") || obj.contains("cdf") || obj.contains("file")) {
                rawActions += line

                // Determine the actual action type and cursor.
                // Delta format wraps everything as "file" with a "deltaSingleAction"
                // sub-object containing the real action type (add/cdc/remove).
                // Parquet format uses "add", "cdc"/"cdf", or "file" directly.
                val (actionType, a) = if (obj.contains("file")) {
                  val fileObj = json.hcursor.downField("file")
                  val dsa = fileObj.downField("deltaSingleAction")
                  if (dsa.succeeded) {
                    // Delta format: look inside deltaSingleAction
                    val dsaObj = dsa.as[io.circe.JsonObject].getOrElse(JsonObject.empty)
                    if (dsaObj.contains("cdc")) ("cdc", dsa.downField("cdc"))
                    else if (dsaObj.contains("add")) ("add", dsa.downField("add"))
                    else if (dsaObj.contains("remove")) ("remove", dsa.downField("remove"))
                    else ("file", fileObj)
                  } else {
                    // Parquet format: "file" is the action itself (OSS reference server)
                    ("file", fileObj)
                  }
                } else {
                  val actionKey =
                    if (obj.contains("cdc")) "cdc"
                    else if (obj.contains("cdf")) "cdf"
                    else "add"
                  (actionKey, json.hcursor.downField(actionKey))
                }

                // Get the URL — for delta format, the URL is in "path" not "url"
                val url = a
                  .get[String]("url")
                  .orElse(a.get[String]("path"))
                  .getOrElse("")

                // Extract deletion vector metadata if present
                // storageType: "u" = UUID-named file relative to table root,
                //              "p" = absolute path or presigned URL,
                //              "i" = inline (base64-encoded in pathOrInlineDv)
                val dv = a.downField("deletionVector")
                val dvUrl = dv.get[String]("storageType").toOption.flatMap {
                  case "u" | "p" => dv.get[String]("pathOrInlineDv").toOption
                  case "i" => dv.get[String]("pathOrInlineDv").toOption
                  case _ => None
                }
                val dvSize = dv.get[Long]("sizeInBytes").toOption
                val dvStorageType = dv.get[String]("storageType").toOption
                val dvOffset = dv.get[Long]("offset").toOption
                val dvCardinality = dv.get[Long]("cardinality").toOption

                // Get version — at the outer "file" level for delta format, or at action level
                val version = if (obj.contains("file")) {
                  json.hcursor.downField("file").get[Long]("version").getOrElse(0L)
                } else {
                  a.get[Long]("version").getOrElse(0L)
                }

                val timestamp = if (obj.contains("file")) {
                  json.hcursor.downField("file").get[Long]("timestamp").getOrElse(0L)
                } else {
                  a.get[Long]("timestamp").getOrElse(0L)
                }

                // Only include actions that have a fetchable URL (skip removes without data)
                if (url.nonEmpty) {
                  files += DeltaSharingFileAction(
                    actionType = actionType,
                    url = url,
                    size = a.get[Long]("size").getOrElse(0L),
                    partitionValues = a
                      .downField("partitionValues")
                      .as[Map[String, String]]
                      .getOrElse(Map.empty),
                    version = version,
                    timestamp = timestamp,
                    deletionVectorFileUrl = dvUrl,
                    deletionVectorSizeInBytes = dvSize,
                    deletionVectorStorageType = dvStorageType,
                    deletionVectorOffset = dvOffset,
                    deletionVectorCardinality = dvCardinality,
                  )
                }
              }
            // Unparseable lines are silently skipped
            case Left(_) => ()
          }
        }

        DeltaSharingChangesResponse(
          protocol,
          metadata,
          files.result(),
          rawProtocolJson = if (requestDeltaFormat) rawProtocol else None,
          rawMetadataJson = if (requestDeltaFormat) rawMetadata else None,
          rawActionJsons = if (requestDeltaFormat) rawActions.result() else Seq.empty,
        )
      }
}

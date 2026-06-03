package com.thatdot.quine.ingest2.sources

import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route

import com.github.mjakubowski84.parquet4s.{BinaryValue, LongValue, ParquetWriter, Path => Pq4sPath, RowParquetRecord}
import org.apache.parquet.schema.MessageTypeParser

/** A mock Delta Sharing server for testing the DeltaSharingCdfSource.
  *
  * Implements enough of the Delta Sharing protocol to test the ingest end-to-end:
  *   - GET  .../version         — returns current version in Delta-Table-Version header
  *   - GET  .../metadata        — returns protocol + metadata NDJSON
  *   - GET  .../changes         — returns CDF file actions as NDJSON
  *   - POST .../query           — returns snapshot file actions as NDJSON
  *   - GET  /files/{name}       — serves Parquet files
  *
  * Use `pushInserts`, `pushUpdates`, `pushDeletes` to add CDF data that will
  * be returned on the next `/changes` call.
  */
class DeltaSharingMockServer(host: String = "127.0.0.1", port: Int = 18080)(implicit system: ActorSystem) {

  implicit private val ec: ExecutionContext = system.dispatcher

  private val currentVersion = new AtomicLong(1L)
  private val tempDir = Files.createTempDirectory("delta-sharing-mock").toFile

  // Pending CDF files: (version, filename, parquet bytes)
  private val pendingCdfFiles = new AtomicReference[Seq[(Long, String, Array[Byte])]](Seq.empty)
  // Snapshot files: (filename, parquet bytes)
  private val snapshotFiles = new AtomicReference[Seq[(String, Array[Byte])]](Seq.empty)

  // All files served by this server (filename -> bytes)
  private val servedFiles = new java.util.concurrent.ConcurrentHashMap[String, Array[Byte]]()

  private val expectedToken = "test-bearer-token-12345"

  private val parquetSchema = MessageTypeParser.parseMessageType(
    """message user_delta {
      |  required int64 id;
      |  optional binary firstname (STRING);
      |  optional binary lastname (STRING);
      |  optional binary email (STRING);
      |  optional binary _change_type (STRING);
      |  optional int64 _commit_version;
      |  optional int64 _commit_timestamp;
      |}""".stripMargin,
  )

  private val snapshotParquetSchema = MessageTypeParser.parseMessageType(
    """message user_delta {
      |  required int64 id;
      |  optional binary firstname (STRING);
      |  optional binary lastname (STRING);
      |  optional binary email (STRING);
      |}""".stripMargin,
  )

  private var bindingFuture: Option[Future[Http.ServerBinding]] = None

  def start(): Unit = {
    bindingFuture = Some(Http().newServerAt(host, port).bind(routes))
    Await.result(bindingFuture.get, 5.seconds)
    println(s"Mock Delta Sharing server started on $host:$port")
  }

  def stop(): Unit =
    bindingFuture.foreach { f =>
      Await.result(f.flatMap(_.unbind()), 5.seconds)
    }

  /** Set the initial snapshot data (for snapshotOnFirstRun / POST .../query). */
  def setSnapshot(rows: Seq[UserRow]): Unit = {
    val filename = s"snapshot-${System.currentTimeMillis()}.parquet"
    val bytes = writeSnapshotParquet(rows)
    servedFiles.put(filename, bytes)
    snapshotFiles.set(Seq((filename, bytes)))
  }

  // ── Fault injection ──

  /** Register a file that returns corrupted (non-Parquet) bytes. */
  def pushCorruptedFile(version: Long): Long = {
    val v = currentVersion.incrementAndGet()
    val filename = s"corrupted-v$v-${System.currentTimeMillis()}.parquet"
    servedFiles.put(filename, "this is not parquet data".getBytes)
    val current = pendingCdfFiles.get()
    pendingCdfFiles.set(current :+ (v, filename, Array.emptyByteArray))
    v
  }

  /** Register a file that will return HTTP 500 when fetched. */
  def pushUnfetchableFile(version: Long): Long = {
    val v = currentVersion.incrementAndGet()
    val filename = s"unfetchable-v$v-${System.currentTimeMillis()}.parquet"
    // Don't put bytes in servedFiles — the /files/ route will 404
    val current = pendingCdfFiles.get()
    pendingCdfFiles.set(current :+ (v, filename, Array.emptyByteArray))
    v
  }

  /** Set whether to use "cdf" (Databricks style) or "cdc" (protocol spec) as the action key. */
  @volatile var useCdfActionKey: Boolean = false

  /** Set whether to use "file" (OSS reference server) or "add" as the snapshot action key. */
  @volatile var useFileActionKey: Boolean = false

  /** Set whether to omit the delta. prefix from configuration keys. */
  @volatile var omitDeltaPrefix: Boolean = false

  /** Set whether /version returns 404 (simulating OSS reference server). */
  @volatile var versionEndpointReturns404: Boolean = false

  /** Set whether to return a deletion vectors error for /changes. */
  @volatile var simulateDeletionVectorError: Boolean = false

  /** Push insert CDF events. Increments version. */
  def pushInserts(rows: Seq[UserRow]): Long = pushCdfEvents(rows, "insert")

  /** Push update CDF events (postimage). Increments version. */
  def pushUpdates(rows: Seq[UserRow]): Long = pushCdfEvents(rows, "update_postimage")

  /** Push delete CDF events. Increments version. */
  def pushDeletes(rows: Seq[UserRow]): Long = pushCdfEvents(rows, "delete")

  private def pushCdfEvents(rows: Seq[UserRow], changeType: String): Long = {
    val version = currentVersion.incrementAndGet()
    val filename = s"cdf-v$version-${System.currentTimeMillis()}.parquet"
    val bytes = writeCdfParquet(rows, changeType, version)
    servedFiles.put(filename, bytes)
    val current = pendingCdfFiles.get()
    pendingCdfFiles.set(current :+ (version, filename, bytes))
    version
  }

  /** Drain pending CDF files for versions in range. */
  private def drainCdfFiles(startVersion: Long, endVersion: Option[Long]): Seq[(Long, String)] = {
    val current = pendingCdfFiles.get()
    val endV = endVersion.getOrElse(Long.MaxValue)
    val (matching, remaining) = current.partition { case (v, _, _) => v >= startVersion && v <= endV }
    pendingCdfFiles.set(remaining)
    matching.map { case (v, name, _) => (v, name) }
  }

  // ── Routes ──

  private def routes: Route =
    concat(
      // File serving — no auth required (simulates presigned URLs)
      path("files" / Segment) { filename =>
        get {
          val bytes = servedFiles.get(filename)
          if (bytes != null) {
            complete(HttpEntity(MediaTypes.`application/octet-stream`, bytes))
          } else {
            complete(StatusCodes.NotFound -> s"File not found: $filename")
          }
        }
      },
      // All other routes require auth
      extractRequest { request =>
        val authHeader = request.headers.find(_.lowercaseName == "authorization")
        val isAuthorized = authHeader.exists(_.value == s"Bearer $expectedToken")

        if (!isAuthorized) {
          complete(StatusCodes.Unauthorized -> "Invalid or missing bearer token")
        } else {
          concat(
            // GET .../version
            path("shares" / Segment / "schemas" / Segment / "tables" / Segment / "version") { (_, _, _) =>
              get {
                if (versionEndpointReturns404) {
                  complete(StatusCodes.NotFound -> "Not found")
                } else {
                  respondWithHeader(RawHeader("Delta-Table-Version", currentVersion.get().toString)) {
                    complete(StatusCodes.OK)
                  }
                }
              }
            },
            // GET .../metadata
            path("shares" / Segment / "schemas" / Segment / "tables" / Segment / "metadata") { (_, _, _) =>
              get {
                respondWithHeader(RawHeader("Delta-Table-Version", currentVersion.get().toString)) {
                  val cdfKey =
                    if (omitDeltaPrefix) "enableChangeDataFeed"
                    else "delta.enableChangeDataFeed"
                  val ndjson =
                    s"""{"protocol":{"minReaderVersion":1}}
                       |{"metaData":{"id":"test-table-id","schemaString":"{}","configuration":{"$cdfKey":"true"}}}""".stripMargin
                  complete(HttpEntity(ContentTypes.`application/json`, ndjson))
                }
              }
            },
            // GET .../changes?startingVersion=N[&endingVersion=M]
            path("shares" / Segment / "schemas" / Segment / "tables" / Segment / "changes") { (_, _, _) =>
              get {
                parameters("startingVersion".as[Long], "endingVersion".as[Long].?) { (startV, endV) =>
                  if (simulateDeletionVectorError) {
                    val errorJson =
                      s"""{"error_code":"INVALID_PARAMETER_VALUE","message":"DS_UNSUPPORTED_DELTA_TABLE_FEATURES: Table features delta.enableDeletionVectors are found in table version: $startV."}"""
                    complete(StatusCodes.BadRequest -> errorJson)
                  } else {
                    val files = drainCdfFiles(startV, endV)
                    val actionKey = if (useCdfActionKey) "cdf" else "cdc"
                    val lines = Seq(
                      """{"protocol":{"minReaderVersion":1}}""",
                      metadataLine,
                    ) ++ files.map { case (version, filename) =>
                      s"""{"$actionKey":{"url":"http://$host:$port/files/$filename","id":"$filename","size":1000,"partitionValues":{},"version":$version,"timestamp":${System
                        .currentTimeMillis()}}}"""
                    }
                    complete(HttpEntity(ContentTypes.`application/json`, lines.mkString("\n")))
                  }
                }
              }
            },
            // POST .../query (snapshot)
            path("shares" / Segment / "schemas" / Segment / "tables" / Segment / "query") { (_, _, _) =>
              post {
                val snapshot = snapshotFiles.get()
                val snapshotActionKey = if (useFileActionKey) "file" else "add"
                val lines = Seq(
                  """{"protocol":{"minReaderVersion":1}}""",
                  metadataLine,
                ) ++ snapshot.map { case (filename, _) =>
                  s"""{"$snapshotActionKey":{"url":"http://$host:$port/files/$filename","id":"$filename","size":1000,"partitionValues":{},"version":${currentVersion
                    .get()},"timestamp":${System.currentTimeMillis()}}}"""
                }
                complete(HttpEntity(ContentTypes.`application/json`, lines.mkString("\n")))
              }
            },
          )
        }
      },
    )

  private def metadataLine: String = {
    val cdfKey =
      if (omitDeltaPrefix) "enableChangeDataFeed"
      else "delta.enableChangeDataFeed"
    s"""{"metaData":{"id":"test-table-id","schemaString":"{}","configuration":{"$cdfKey":"true"}}}"""
  }

  // ── Parquet writing ──

  private def writeCdfParquet(rows: Seq[UserRow], changeType: String, version: Long): Array[Byte] = {
    val file = new File(tempDir, s"cdf-${System.nanoTime()}.parquet")
    try {
      val ts = System.currentTimeMillis()
      val records = rows.map { row =>
        RowParquetRecord
          .emptyWithSchema(
            "id",
            "firstname",
            "lastname",
            "email",
            "_change_type",
            "_commit_version",
            "_commit_timestamp",
          )
          .updated("id", LongValue(row.id))
          .updated("firstname", BinaryValue(row.firstname))
          .updated("lastname", BinaryValue(row.lastname))
          .updated("email", BinaryValue(row.email))
          .updated("_change_type", BinaryValue(changeType))
          .updated("_commit_version", LongValue(version))
          .updated("_commit_timestamp", LongValue(ts))
      }

      ParquetWriter
        .generic(parquetSchema)
        .writeAndClose(Pq4sPath(file.toPath.toString), records)

      Files.readAllBytes(file.toPath)
    } finally {
      val _ = file.delete()
    }
  }

  private def writeSnapshotParquet(rows: Seq[UserRow]): Array[Byte] = {
    val file = new File(tempDir, s"snapshot-${System.nanoTime()}.parquet")
    try {
      val records = rows.map { row =>
        RowParquetRecord
          .emptyWithSchema("id", "firstname", "lastname", "email")
          .updated("id", LongValue(row.id))
          .updated("firstname", BinaryValue(row.firstname))
          .updated("lastname", BinaryValue(row.lastname))
          .updated("email", BinaryValue(row.email))
      }

      ParquetWriter
        .generic(snapshotParquetSchema)
        .writeAndClose(Pq4sPath(file.toPath.toString), records)

      Files.readAllBytes(file.toPath)
    } finally {
      val _ = file.delete()
    }
  }

  def cleanup(): Unit = {
    tempDir.listFiles().foreach(_.delete())
    val _ = tempDir.delete()
  }
}

/** Simple row model for test data. */
case class UserRow(
  id: Long,
  firstname: String,
  lastname: String,
  email: String,
)

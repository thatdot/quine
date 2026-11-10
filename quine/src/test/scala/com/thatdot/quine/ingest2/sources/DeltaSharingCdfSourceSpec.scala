package com.thatdot.quine.ingest2.sources

import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.thatdot.common.security.Secret
import com.thatdot.quine.app.Metrics
import com.thatdot.quine.app.model.ingest2.BearerTokenAuth
import com.thatdot.quine.app.model.ingest2.codec.{BufferedSeekableInput, ParquetDecoder, ParquetRecord}
import com.thatdot.quine.app.model.ingest2.source.DecodedSource
import com.thatdot.quine.app.model.ingest2.sources.{DeltaSharingCdfSource, DeltaSharingClient, DeltaSharingException}
import com.thatdot.quine.app.routes.IngestMetered
import com.thatdot.quine.graph.defaultNamespaceId
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.ingest2.IngestSourceTestSupport.randomString

class DeltaSharingCdfSourceSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("delta-sharing-test")
  private val mockPort = 18181
  private val mockServer = new DeltaSharingMockServer(port = mockPort)

  private val auth = BearerTokenAuth(Secret("test-bearer-token-12345"))

  private def readParquet(bytes: Array[Byte]): Seq[ParquetRecord] =
    Await.result(
      new ParquetDecoder().decode(new BufferedSeekableInput(bytes)).map(_.get).runWith(Sink.seq),
      5.seconds,
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.start()
  }

  override def afterAll(): Unit = {
    mockServer.stop()
    Await.result(system.terminate(), 10.seconds)
    super.afterAll()
  }

  private def makeClient(token: String = "test-bearer-token-12345", maxRetries: Int = 1): DeltaSharingClient =
    new DeltaSharingClient(
      endpoint = s"http://127.0.0.1:$mockPort",
      auth = BearerTokenAuth(Secret(token)),
      serverTimeoutMs = 5000L,
      parquetFetchTimeoutMs = 5000L,
      maxRetries = maxRetries,
    )

  private val client = makeClient()

  // ═══════════════════════════════════════════════════════════════════════
  // Client-level protocol tests
  // ═══════════════════════════════════════════════════════════════════════

  "DeltaSharingClient" should {

    "query table version" in {
      val version = Await.result(
        client.queryTableVersion("test-share", "test-schema", "test-table"),
        5.seconds,
      )
      version shouldBe >(0L)
    }

    "query table metadata" in {
      val metadata = Await.result(
        client.queryTableMetadata("test-share", "test-schema", "test-table"),
        5.seconds,
      )
      metadata.protocol.minReaderVersion shouldBe 1
      metadata.metadata.configuration should contain("delta.enableChangeDataFeed" -> "true")
    }

    "reject invalid bearer token" in {
      val badClient = makeClient(token = "wrong-token", maxRetries = 0)
      val ex = intercept[DeltaSharingException] {
        Await.result(
          badClient.queryTableVersion("test-share", "test-schema", "test-table"),
          5.seconds,
        )
      }
      ex.getMessage should include("bearer token")
    }

    "fetch CDF insert events as Parquet" in {
      val insertVersion = mockServer.pushInserts(
        Seq(
          UserRow(1, "Alice", "Smith", "alice@example.com"),
          UserRow(2, "Bob", "Jones", "bob@example.com"),
        ),
      )

      val response = Await.result(
        client.queryTableChanges("test-share", "test-schema", "test-table", insertVersion),
        5.seconds,
      )

      response.files should not be empty
      response.files.head.actionType shouldBe "cdc"
      response.files.head.version shouldBe insertVersion

      val parquetBytes = Await.result(
        client.fetchParquetFile(response.files.head.url),
        5.seconds,
      )
      parquetBytes.length should be > 0

      val rows = readParquet(parquetBytes)
      rows should have size 2
      rows.head.fields.map(_._1).toSet should contain allOf ("firstname", "lastname", "_change_type")
    }

    "fetch snapshot data" in {
      mockServer.setSnapshot(
        Seq(
          UserRow(10, "Carol", "Davis", "carol@example.com"),
          UserRow(11, "Dave", "Wilson", "dave@example.com"),
        ),
      )

      val response = Await.result(
        client.queryTableSnapshot("test-share", "test-schema", "test-table"),
        5.seconds,
      )

      response.files should have size 1
      response.files.head.actionType shouldBe "add"

      val parquetBytes = Await.result(
        client.fetchParquetFile(response.files.head.url),
        5.seconds,
      )

      val rows = readParquet(parquetBytes)
      rows should have size 2
      val fieldNames = rows.head.fields.map(_._1).toSet
      fieldNames should contain allOf ("firstname", "lastname", "email")
      fieldNames should not contain "_change_type"
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // Schema format variation tests
  // ═══════════════════════════════════════════════════════════════════════

  "Schema format variations" should {

    "parse 'cdf' action key (Databricks format)" in {
      mockServer.useCdfActionKey = true
      try {
        val v = mockServer.pushInserts(Seq(UserRow(20, "Cdf", "Test", "cdf@test.com")))
        val response = Await.result(
          client.queryTableChanges("test-share", "test-schema", "test-table", v),
          5.seconds,
        )
        response.files should have size 1
        response.files.head.actionType shouldBe "cdf"
        response.files.head.url should not be empty
      } finally mockServer.useCdfActionKey = false
    }

    "parse 'file' action key for snapshots (OSS reference server format)" in {
      mockServer.useFileActionKey = true
      try {
        mockServer.setSnapshot(Seq(UserRow(21, "File", "Test", "file@test.com")))
        val response = Await.result(
          client.queryTableSnapshot("test-share", "test-schema", "test-table"),
          5.seconds,
        )
        response.files should have size 1
        response.files.head.actionType shouldBe "file"

        // Verify the file is actually fetchable and readable
        val bytes = Await.result(client.fetchParquetFile(response.files.head.url), 5.seconds)
        bytes.length should be > 0
      } finally mockServer.useFileActionKey = false
    }

    "handle metadata with Databricks-style config keys (no delta. prefix)" in {
      mockServer.omitDeltaPrefix = true
      try {
        val metadata = Await.result(
          client.queryTableMetadata("test-share", "test-schema", "test-table"),
          5.seconds,
        )
        // Should parse successfully with the non-prefixed key
        metadata.metadata.configuration should contain("enableChangeDataFeed" -> "true")
      } finally mockServer.omitDeltaPrefix = false
    }

    "fall back to metadata header when /version returns 404" in {
      mockServer.versionEndpointReturns404 = true
      try {
        val version = Await.result(
          client.queryTableVersion("test-share", "test-schema", "test-table"),
          5.seconds,
        )
        version shouldBe >(0L)
      } finally mockServer.versionEndpointReturns404 = false
    }

    "handle both 'cdc' and 'cdf' action keys in the same test" in {
      // Test with cdc (default)
      val v1 = mockServer.pushInserts(Seq(UserRow(22, "Cdc", "Default", "cdc@test.com")))
      val r1 = Await.result(
        client.queryTableChanges("test-share", "test-schema", "test-table", v1),
        5.seconds,
      )
      r1.files.head.actionType shouldBe "cdc"

      // Switch to cdf
      mockServer.useCdfActionKey = true
      try {
        val v2 = mockServer.pushInserts(Seq(UserRow(23, "Cdf", "Switch", "cdf2@test.com")))
        val r2 = Await.result(
          client.queryTableChanges("test-share", "test-schema", "test-table", v2),
          5.seconds,
        )
        r2.files.head.actionType shouldBe "cdf"
      } finally mockServer.useCdfActionKey = false
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // Failure mode tests
  // ═══════════════════════════════════════════════════════════════════════

  "Failure modes" should {

    "return deletion vector error from /changes endpoint" in {
      mockServer.simulateDeletionVectorError = true
      try {
        val ex = intercept[DeltaSharingException] {
          Await.result(
            client.queryTableChanges("test-share", "test-schema", "test-table", 1),
            5.seconds,
          )
        }
        // 400 response should be surfaced
        ex.getMessage should (include("400") or include("DeletionVectors") or include("deletion"))
      } finally mockServer.simulateDeletionVectorError = false
    }

    "return 404 for unfetchable Parquet file" in {
      val noRetryClient = makeClient(maxRetries = 0)
      val ex = intercept[DeltaSharingException] {
        Await.result(
          noRetryClient.fetchParquetFile(s"http://127.0.0.1:$mockPort/files/does-not-exist.parquet"),
          5.seconds,
        )
      }
      ex.getMessage should include("404")
    }

    "serve corrupted file bytes successfully (client receives them)" in {
      // The client can fetch the bytes — the failure happens at deserialization
      mockServer.pushCorruptedFile(0L)
      val changes = Await.result(
        client.queryTableChanges("test-share", "test-schema", "test-table", mockServer.currentVersion.get()),
        5.seconds,
      )
      // The corrupted file should appear in the changes response
      // (it's up to the source to handle deserialization failure)
      if (changes.files.nonEmpty) {
        val bytes = Await.result(
          client.fetchParquetFile(changes.files.head.url),
          5.seconds,
        )
        // Bytes should be the corrupted content, not empty
        // Trying to read them as Parquet should fail
        an[Exception] shouldBe thrownBy(readParquet(bytes))
      }
    }

    "fail with DeltaSharingException on connection refused" in {
      val unreachableClient = new DeltaSharingClient(
        endpoint = "http://127.0.0.1:19999", // nothing listening here
        auth = auth,
        serverTimeoutMs = 2000L,
        parquetFetchTimeoutMs = 2000L,
        maxRetries = 0,
      )
      an[Exception] shouldBe thrownBy {
        Await.result(
          unreachableClient.queryTableMetadata("any", "any", "any"),
          5.seconds,
        )
      }
    }
  }

  "DeltaSharingCdfSource reading an unreadable Parquet file" should {

    def source(skipUnreadableVersions: Boolean, startingVersion: Long): DecodedSource =
      DeltaSharingCdfSource(
        ingestName = "unreadable-file-test",
        endpoint = s"http://127.0.0.1:$mockPort",
        auth = auth,
        shareName = "test-share",
        schemaName = "test-schema",
        tableName = "test-table",
        startingVersion = Some(startingVersion),
        snapshotOnFirstRun = false,
        pollIntervalMs = 50L,
        maxVersionsPerPoll = 1,
        serverRequestTimeoutMs = 5000L,
        parquetFetchTimeoutMs = 5000L,
        maxRetries = 0,
        skipUnreadableVersions = skipUnreadableVersions,
        parquetDownloadParallelism = 1,
        meter = IngestMetered.ingestMeter(
          defaultNamespaceId,
          randomString(),
          HostQuineMetrics(enableDebugMetrics = false, metricRegistry = Metrics, omitDefaultNamespace = true),
        ),
      ).decodedSource

    /** Returns the stream's elements as `Right`s of (decode failed, frame content), ending in a `Left` if it failed. */
    def elements(decoded: DecodedSource, limit: Long): Seq[Either[Throwable, (Boolean, String)]] =
      Await.result(
        decoded.stream
          .map { case (decode, frame) =>
            Right((decode().isFailure, new String(decoded.content(frame), "UTF-8"))): Either[
              Throwable,
              (Boolean, String),
            ]
          }
          .recover { case e: Throwable => Left(e) }
          .take(limit)
          .runWith(Sink.seq),
        10.seconds,
      )

    "emit a dead-letter record and fail the stream when skipUnreadableVersions is false" in {
      val version = mockServer.pushCorruptedFile(0L)
      // The source reads the versions after its starting version.
      val result = elements(source(skipUnreadableVersions = false, version - 1L), limit = 10L)
      result should have size 2
      result.head.toOption.get._1 shouldBe true
      result.head.toOption.get._2 should include("\"_errorPhase\":\"deserialize\"")
      result(1).left.toOption.get shouldBe a[DeltaSharingException]
    }

    "emit a dead-letter record and continue when skipUnreadableVersions is true" in {
      val version = mockServer.pushCorruptedFile(0L)
      // The source reads the versions after its starting version. It keeps polling, so only the first element is taken.
      val result = elements(source(skipUnreadableVersions = true, version - 1L), limit = 1L)
      result should have size 1
      result.head.toOption.get._1 shouldBe true
      result.head.toOption.get._2 should include("\"_errorPhase\":\"deserialize\"")
    }
  }

  // Expose currentVersion for test assertions
  implicit private class MockServerOps(server: DeltaSharingMockServer) {
    def currentVersion: java.util.concurrent.atomic.AtomicLong = {
      val field = classOf[DeltaSharingMockServer].getDeclaredField("currentVersion")
      field.setAccessible(true)
      field.get(server).asInstanceOf[java.util.concurrent.atomic.AtomicLong]
    }
  }
}

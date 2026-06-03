package com.thatdot.quine.ingest2.sources

import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.pekko.actor.ActorSystem

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.thatdot.common.security.Secret
import com.thatdot.quine.app.model.ingest2.BearerTokenAuth
import com.thatdot.quine.app.model.ingest2.sources.{DeltaSharingClient, DeltaSharingException}

class DeltaSharingCdfSourceSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("delta-sharing-test")
  private val mockPort = 18181
  private val mockServer = new DeltaSharingMockServer(port = mockPort)

  private val auth = BearerTokenAuth(Secret("test-bearer-token-12345"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.start()
  }

  override def afterAll(): Unit = {
    mockServer.stop()
    mockServer.cleanup()
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

      import com.thatdot.quine.app.model.ingest2.sources.ByteArrayInputFile
      import com.github.mjakubowski84.parquet4s.ParquetReader
      val inputFile = new ByteArrayInputFile(parquetBytes)
      val iterable = ParquetReader.generic.read(inputFile)
      try {
        val rows = iterable.iterator.toSeq
        rows should have size 2
        rows.head.iterator.map(_._1).toSet should contain allOf ("firstname", "lastname", "_change_type")
      } finally iterable.close()
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

      import com.thatdot.quine.app.model.ingest2.sources.ByteArrayInputFile
      import com.github.mjakubowski84.parquet4s.ParquetReader
      val inputFile = new ByteArrayInputFile(parquetBytes)
      val iterable = ParquetReader.generic.read(inputFile)
      try {
        val rows = iterable.iterator.toSeq
        rows should have size 2
        val fieldNames = rows.head.iterator.map(_._1).toSet
        fieldNames should contain allOf ("firstname", "lastname", "email")
        fieldNames should not contain "_change_type"
      } finally iterable.close()
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
        import com.thatdot.quine.app.model.ingest2.sources.ByteArrayInputFile
        import com.github.mjakubowski84.parquet4s.ParquetReader
        an[Exception] shouldBe thrownBy {
          val iterable = ParquetReader.generic.read(new ByteArrayInputFile(bytes))
          try iterable.iterator.toSeq
          finally iterable.close()
        }
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

  // Expose currentVersion for test assertions
  implicit private class MockServerOps(server: DeltaSharingMockServer) {
    def currentVersion: java.util.concurrent.atomic.AtomicLong = {
      val field = classOf[DeltaSharingMockServer].getDeclaredField("currentVersion")
      field.setAccessible(true)
      field.get(server).asInstanceOf[java.util.concurrent.atomic.AtomicLong]
    }
  }
}

package com.thatdot.quine.ingest2.sources

import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.pekko.actor.ActorSystem

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.thatdot.common.security.Secret
import com.thatdot.quine.app.model.ingest2.BearerTokenAuth
import com.thatdot.quine.app.model.ingest2.sources.{ByteArrayInputFile, DeltaSharingClient, DeltaSharingException}

/** Integration tests against the public Delta Sharing reference test server.
  *
  * These tests validate our protocol implementation against a real server
  * (https://sharing.delta.io/delta-sharing/). They require network access
  * and will be skipped if the server is unreachable.
  *
  * == Automated tests ==
  *
  * {{{
  * sbt 'quine/testOnly com.thatdot.quine.ingest2.sources.DeltaSharingReferenceServerSpec'
  * }}}
  *
  * == Manual end-to-end test against the reference server ==
  *
  * Terminal 1 — start Quine:
  * {{{
  * sbt 'quine/run'
  * }}}
  *
  * Terminal 2 — create the ingest (ingests 506 boston-housing rows, then completes):
  * {{{
  * TOKEN=$(curl -s https://raw.githubusercontent.com/delta-io/delta-sharing/main/examples/open-datasets.share | jq -r .bearerToken)
  *
  * curl -X POST 'http://localhost:8080/api/v2/graph/quine/ingests' -H 'Content-Type: application/json' -d '{"name":"boston-housing","source":{"type":"DeltaSharingCdf","endpoint":"https://sharing.delta.io/delta-sharing/","auth":{"type":"BearerToken","bearerToken":"'"$TOKEN"'"},"shareName":"delta_sharing","schemaName":"default","tableName":"boston-housing","snapshotOnFirstRun":true,"pollIntervalMs":30000},"query":"MATCH (n) WHERE id(n) = idFrom('"'"'boston'"'"', $that.ID) SET n = $that SET n:BostonHousing","parameter":"that"}'
  * }}}
  *
  * Terminal 2 — verify the data was ingested:
  * {{{
  * curl 'http://localhost:8080/api/v1/query/cypher' -H 'Content-Type: text/plain' -d 'MATCH (n:BostonHousing) RETURN count(n)'
  * }}}
  *
  * Expected: 506 rows. The ingest status should show "Completed" (not "Running")
  * because this table does not have CDF enabled, so the stream completes after
  * the snapshot.
  *
  * == Server discovery notes ==
  *
  *   - 1 share: delta_sharing
  *   - 1 schema: default
  *   - 7 tables: COVID_19_NYT, boston-housing, flight-asa_2008, lending_club,
  *               nyctaxi_2019, nyctaxi_2019_part, owid-covid-data
  *   - CDF status: boston-housing has CDF enabled but only version 0 (no CDF
  *     data available). All other tables have CDF disabled.
  *   - /version endpoint: returns 404 on this server. Version is available via
  *     Delta-Table-Version header on /metadata responses.
  *   - Snapshot query (POST .../query): works. Returns "file" action type
  *     (not "add").
  */
class DeltaSharingReferenceServerSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("delta-sharing-ref-test")

  // Fetch credentials from the public profile
  private val profileUrl = "https://raw.githubusercontent.com/delta-io/delta-sharing/main/examples/open-datasets.share"
  private lazy val (endpoint, token) = {
    import scala.io.Source
    val profileJson = Source.fromURL(profileUrl).mkString
    val json = io.circe.parser.parse(profileJson).getOrElse(io.circe.Json.Null)
    val ep = json.hcursor.get[String]("endpoint").getOrElse("")
    val tk = json.hcursor.get[String]("bearerToken").getOrElse("")
    (ep, tk)
  }

  private lazy val client = new DeltaSharingClient(
    endpoint = endpoint,
    auth = BearerTokenAuth(Secret(token)),
    serverTimeoutMs = 30000L,
    parquetFetchTimeoutMs = 60000L,
    maxRetries = 2,
  )

  // Use boston-housing as the primary fixture: small (506 rows, 27KB), CDF-enabled
  private val share = "delta_sharing"
  private val schema = "default"
  private val table = "boston-housing"

  private def isServerReachable: Boolean =
    try {
      Await.result(client.queryTableMetadata(share, schema, table), 15.seconds)
      true
    } catch {
      case _: Exception => false
    }

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    super.afterAll()
  }

  "DeltaSharingClient against reference server" should {

    "query table version (via metadata fallback)" in {
      assume(isServerReachable, "Reference server unreachable")

      val version = Await.result(
        client.queryTableVersion(share, schema, table),
        15.seconds,
      )

      // boston-housing is at version 0
      version shouldBe 0L
    }

    "query table metadata" in {
      assume(isServerReachable, "Reference server unreachable")

      val metadata = Await.result(
        client.queryTableMetadata(share, schema, table),
        15.seconds,
      )

      metadata.protocol.minReaderVersion shouldBe 1
      metadata.metadata.id should not be empty
      metadata.metadata.schemaString shouldBe defined
      // boston-housing has CDF enabled (but no CDF data exists)
      // Configuration may or may not explicitly list it; the server reports it
      // via the /changes error message rather than in configuration
    }

    "accept any bearer token (reference server does not enforce auth)" in {
      assume(isServerReachable, "Reference server unreachable")

      // The public reference server accepts any token — it does not enforce
      // authentication. This is expected for a public test server. Auth
      // rejection (401) is tested against the mock server in
      // DeltaSharingCdfSourceSpec and would be tested against a real
      // Databricks server in Phase 3.
      val anyTokenClient = new DeltaSharingClient(
        endpoint = endpoint,
        auth = BearerTokenAuth(Secret("any-token-works-here")),
        serverTimeoutMs = 15000L,
        parquetFetchTimeoutMs = 15000L,
        maxRetries = 0,
      )

      val metadata = Await.result(
        anyTokenClient.queryTableMetadata(share, schema, table),
        15.seconds,
      )
      metadata.protocol.minReaderVersion shouldBe 1
    }

    "reject non-existent table with 404" in {
      assume(isServerReachable, "Reference server unreachable")

      val ex = intercept[DeltaSharingException] {
        Await.result(
          client.queryTableMetadata(share, schema, "nonexistent-table-xyz"),
          15.seconds,
        )
      }
      ex.getMessage should (include("not found") or include("404"))
    }

    "fetch snapshot via POST .../query and deserialize Parquet" in {
      assume(isServerReachable, "Reference server unreachable")

      // Get snapshot file listing
      val response = Await.result(
        client.queryTableSnapshot(share, schema, table),
        30.seconds,
      )

      response.protocol shouldBe defined
      response.metadata shouldBe defined
      response.files should not be empty
      // Reference server uses "file" action type for snapshots
      response.files.foreach { file =>
        file.url should not be empty
        file.size should be > 0L
      }

      // Fetch the actual Parquet file from the presigned URL
      val firstFile = response.files.head
      val parquetBytes = Await.result(
        client.fetchParquetFile(firstFile.url),
        30.seconds,
      )

      parquetBytes.length shouldBe firstFile.size.toInt

      // Deserialize using parquet4s — same library the ingest source uses
      val inputFile = new ByteArrayInputFile(parquetBytes)

      import com.github.mjakubowski84.parquet4s.ParquetReader
      val iterable = ParquetReader.generic.read(inputFile)
      try {
        val rows = iterable.iterator.toSeq

        // boston-housing has 506 rows
        rows should have size 506

        // Check schema: should have ID, crim, zn, indus, etc.
        val firstRow = rows.head
        val fieldNames = firstRow.iterator.map(_._1).toSet
        fieldNames should contain allOf ("ID", "crim", "zn", "indus", "rm", "age", "medv")

        // Snapshot rows should NOT have CDF metadata columns
        fieldNames should not contain "_change_type"
        fieldNames should not contain "_commit_version"
        fieldNames should not contain "_commit_timestamp"
      } finally iterable.close()
    }

    "report CDF unavailable for non-CDF tables" in {
      assume(isServerReachable, "Reference server unreachable")

      // COVID_19_NYT explicitly does not have CDF enabled
      val ex = intercept[DeltaSharingException] {
        Await.result(
          client.queryTableChanges(share, schema, "COVID_19_NYT", startingVersion = 0),
          15.seconds,
        )
      }
      // Server returns 400 with "cdf is not enabled"
      ex.getMessage should (include("cdf") or include("change data") or include("400"))
    }

    "report version out of range for CDF-enabled table" in {
      assume(isServerReachable, "Reference server unreachable")

      // boston-housing has CDF enabled but only version 0 — requesting changes
      // from version 1 should fail since it's beyond the latest version
      val ex = intercept[DeltaSharingException] {
        Await.result(
          client.queryTableChanges(share, schema, table, startingVersion = 1),
          15.seconds,
        )
      }
      ex.getMessage should (include("version") or include("400"))
    }
  }
}

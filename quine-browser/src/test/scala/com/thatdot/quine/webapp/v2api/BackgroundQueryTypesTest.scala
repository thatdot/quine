package com.thatdot.quine.webapp.v2api

import io.circe.parser.decode
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.webapp.v2api.V2ApiTypes.{V2BackgroundQueryStatus, V2JobStatus}

/** Pins the browser's wire mirrors against payloads captured verbatim from a running server
  * (`GET /api/v2/graph/quine/backgroundQueries`, `GET /api/v2/system/jobs`).
  *
  * These types are hand-written mirrors of JVM-only API types, so nothing but a test like this
  * catches drift — a renamed or newly-omitted field turns into a silently empty UI, not a
  * compile error.
  */
class BackgroundQueryTypesTest extends AnyFunSuite with Matchers with OptionValues {

  test("a completed execution decodes, including the omitted optional fields") {
    // Captured from the server: note `jobName` and `error` are absent rather than null.
    val json =
      """{
        |  "id": "c5c3c688-60a4-41a0-b24d-45db1a3f08d4",
        |  "name": "seed-sweep",
        |  "query": "MATCH (n:T) RETURN n.i",
        |  "status": "completed",
        |  "hostId": "local",
        |  "totalRowCount": 3000,
        |  "columns": ["n.i"],
        |  "expiresAt": "2026-08-18T19:59:24.938Z"
        |}""".stripMargin

    val run = decode[V2BackgroundQueryStatus](json).toOption.value
    run.id shouldBe "c5c3c688-60a4-41a0-b24d-45db1a3f08d4"
    run.jobName shouldBe None
    run.error shouldBe None
    run.totalRowCount.value shouldBe 3000L
    run.columns.value should contain only "n.i"
    run.isRunning shouldBe false
    run.displayName shouldBe "seed-sweep"
  }

  test("an in-flight execution has no row count and reads as running") {
    val json =
      """{
        |  "id": "5db28974-a17b-44f1-8fc7-8d07ee32e2eb",
        |  "query": "MATCH (n:T) RETURN n.i",
        |  "status": "started",
        |  "hostId": "local",
        |  "expiresAt": "2026-08-18T19:59:24.938Z"
        |}""".stripMargin

    val run = decode[V2BackgroundQueryStatus](json).toOption.value
    run.isRunning shouldBe true
    run.totalRowCount shouldBe None
    // No name on the wire, so the query text stands in — collapsed to one line for the menu row.
    run.displayName shouldBe "MATCH (n:T) RETURN n.i"
  }

  test("displayName collapses whitespace in a multi-line query") {
    val json =
      """{"id":"x","query":"MATCH (n)\n  RETURN n","status":"started","hostId":"local",
        |"expiresAt":"2026-08-18T19:59:24.938Z"}""".stripMargin
    decode[V2BackgroundQueryStatus](json).toOption.value.displayName shouldBe "MATCH (n) RETURN n"
  }

  test("an interval job decodes and renders its schedule") {
    // Captured from the server, including the `startAt` it fills in on creation.
    val json =
      """{
        |  "name": "nightly-rollup",
        |  "jobType": "background-query",
        |  "schedule": {"every": "30s", "startAt": "2026-08-11T20:22:16.848Z", "type": "Interval"},
        |  "nextFireAt": "2026-08-11T20:22:46.848Z",
        |  "lastFireAt": "2026-08-11T20:22:16.860Z",
        |  "running": true
        |}""".stripMargin

    val job = decode[V2JobStatus](json).toOption.value
    job.name shouldBe "nightly-rollup"
    job.jobType shouldBe "background-query"
    job.running shouldBe true
    job.scheduleSummary shouldBe "Every 30s"
  }

  test("the wall-clock schedule variants each render") {
    def summaryOf(schedule: String): String = {
      val json = s"""{"name":"j","jobType":"background-query","schedule":$schedule,"running":false}"""
      decode[V2JobStatus](json).toOption.value.scheduleSummary
    }

    summaryOf("""{"type":"Hourly","minute":5,"timezone":"UTC"}""") shouldBe "Hourly at :05 (UTC)"
    summaryOf("""{"type":"Daily","at":"02:00","timezone":"UTC"}""") shouldBe "Daily at 02:00 (UTC)"
    summaryOf("""{"type":"Weekly","dayOfWeek":"MONDAY","at":"09:30","timezone":"America/New_York"}""") shouldBe
    "Weekly on Monday at 09:30 (America/New_York)"
    summaryOf("""{"type":"Monthly","dayOfMonth":1,"at":"00:15","timezone":"UTC"}""") shouldBe
    "Monthly on day 1 at 00:15 (UTC)"
  }

  test("a job that has never fired decodes with absent fire times") {
    val json =
      """{"name":"j","jobType":"background-query","schedule":{"type":"Daily","at":"02:00","timezone":"UTC"},
        |"running":false}""".stripMargin
    val job = decode[V2JobStatus](json).toOption.value
    job.nextFireAt shouldBe None
    job.lastFireAt shouldBe None
  }

  test("a schedule variant added server-side later degrades to its discriminator, not a blank") {
    val json = """{"name":"j","jobType":"background-query","schedule":{"type":"Fortnightly"},"running":false}"""
    decode[V2JobStatus](json).toOption.value.scheduleSummary shouldBe "Fortnightly"
  }
}

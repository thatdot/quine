package com.thatdot.quine.webapp.components.streams

import io.circe.Json
import io.circe.parser.parse
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The job-creation request body.
  *
  * Two fields are merged in rather than rendered — `name` from a dedicated input, and
  * `action.namespace` from the page's graph selector — and the second nests inside an object the
  * schema renderer built. Getting that merge wrong would replace the action wholesale, dropping
  * the query and the discriminator, which the server would reject in a way that points nowhere
  * near this code.
  */
class CreateJobFormTest extends AnyFunSuite with Matchers with OptionValues {

  /** What the schema renderer leaves in form state for a typical job. */
  private val renderedForm =
    """{
      |  "schedule": {"type": "Interval", "every": "30s"},
      |  "action": {
      |    "type": "BackgroundQuery",
      |    "query": "MATCH (n) RETURN count(n)",
      |    "destinations": [{"type": "Drop"}]
      |  },
      |  "updateIfExists": false
      |}""".stripMargin

  private def body(formState: String = renderedForm, name: String = "nightly", namespace: String = "quine"): Json =
    CreateJobForm.buildBody(parse(formState).toOption.value, name, namespace)

  test("the selected graph lands on the action") {
    body(namespace = "tenant-a").hcursor
      .downField("action")
      .downField("namespace")
      .as[String] shouldBe Right("tenant-a")
  }

  test("merging the graph in leaves the rest of the action intact") {
    val action = body().hcursor.downField("action")

    action.downField("type").as[String] shouldBe Right("BackgroundQuery")
    action.downField("query").as[String] shouldBe Right("MATCH (n) RETURN count(n)")
    action.downField("destinations").as[List[Json]].toOption.value should have size 1
    action.keys.map(_.toSet).value shouldBe Set("type", "query", "destinations", "namespace")
  }

  test("the schedule is untouched") {
    val schedule = body().hcursor.downField("schedule")
    schedule.downField("type").as[String] shouldBe Right("Interval")
    schedule.downField("every").as[String] shouldBe Right("30s")
  }

  test("the name is merged in and trimmed") {
    body(name = "  nightly-rollup  ").hcursor.downField("name").as[String] shouldBe Right("nightly-rollup")
  }

  test("a graph selected later overrides one already in form state") {
    // `namespace` is hidden from the form, but a stale value could survive a schema change or a
    // variant switch; the selector is the authority either way.
    val stale = """{"action":{"type":"BackgroundQuery","query":"q","namespace":"old"}}"""
    body(formState = stale, namespace = "current").hcursor
      .downField("action")
      .downField("namespace")
      .as[String] shouldBe Right("current")
  }

  test("no other top-level keys are invented") {
    body().hcursor.keys.map(_.toSet).value shouldBe Set("schedule", "action", "updateIfExists", "name")
  }
}

package com.thatdot.quine.app

import java.util.UUID

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

import org.apache.pekko.util.Timeout

import cats.data.NonEmptyList
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import shapeless.{:+:, CNil, Inl, Inr}

import com.thatdot.api.v2.ErrorResponse.{BadRequest, NotFound}
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.config.{FileAccessPolicy, QuineConfig, ResolutionMode}
import com.thatdot.quine.app.model.outputs2.query.standing.LocalTapBus
import com.thatdot.quine.app.v2api.OssApiMethods
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.endpoints.V2BackgroundQueryEndpointEntities.BackgroundQueryDef
import com.thatdot.quine.app.v2api.endpoints.V2JobEndpointEntities.CreateJobRequest
import com.thatdot.quine.app.v2api.endpoints.{Action, Schedule}
import com.thatdot.quine.graph.{GraphService, defaultNamespaceId}

/** Drives the V2 endpoint logic layer end to end against a real graph: run-now background queries,
  * cancellation, job creation + status, the jobName listing filter, and validation.
  */
class JobEndpointMethodsTest extends AnyFunSuite with BeforeAndAfterAll with Eventually {

  implicit val logConfig: LogConfig = LogConfig.permissive
  private val graph: GraphService = IngestTestGraph.makeGraph("job-endpoint-test")
  implicit val ec: ExecutionContext = graph.nodeDispatcherEC

  private val app =
    new QuineApp(
      graph,
      helpMakeQuineBetter = false,
      FileAccessPolicy(List.empty, ResolutionMode.Dynamic),
      new LocalTapBus,
    )
  private val apiMethods = new OssApiMethods(graph, app, QuineConfig(), Timeout(10.seconds))

  /** Result rows go to `Drop` — these tests exercise scheduling/status/validation, not delivery. */
  private val drop: NonEmptyList[QuineDestinationSteps] = NonEmptyList.one(QuineDestinationSteps.Drop)

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(100, Millis))

  override def beforeAll(): Unit = {
    while (!graph.isReady) Thread.sleep(10)
    // Rehydrate the scheduler (as QuineApp.loadAppData does at startup): operations arriving before
    // the load resolves are buffered, so an unstarted scheduler would time every request out.
    Await.result(app.jobScheduler.start(), 10.seconds)
  }

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), 10.seconds)

  test("runBackgroundQuery runs the query and getBackgroundQuery reports completion") {
    val id = Await
      .result(
        apiMethods.runBackgroundQuery(defaultNamespaceId, BackgroundQueryDef("CREATE (:EndpointTest) RETURN 1", drop)),
        5.seconds,
      )
      .getOrElse(fail("expected an id"))

    val status = eventually {
      Await.result(apiMethods.getBackgroundQuery(defaultNamespaceId, id), 5.seconds) match {
        case Right(s) if s.status == "completed" || s.status == "failed" => s
        case Right(s) => fail(s"not terminal yet: ${s.status}")
        case Left(_) => fail("no record yet")
      }
    }
    assert(status.status == "completed", status.error)
    assert(status.id == id)
    assert(status.jobName.isEmpty, "a direct run has no dispatching job")
    assert(status.totalRowCount.contains(1L))
  }

  test("getBackgroundQuery returns NotFound for an unknown id") {
    assert(Await.result(apiMethods.getBackgroundQuery(defaultNamespaceId, UUID.randomUUID()), 5.seconds).isLeft)
  }

  test("createJob dispatches executions, discoverable via the jobName filter; getJob reports the definition") {
    val jobName = Await
      .result(
        apiMethods.createJob(
          CreateJobRequest(
            schedule = Schedule.Interval(1.hour, None), // fires immediately on creation, then hourly
            action = Action.BackgroundQuery(query = "CREATE (:JobTick) RETURN 1", destinations = drop),
            name = "tick job",
          ),
        ),
        5.seconds,
      )
      .getOrElse(fail("expected a job name"))

    try {
      // The immediate first fire dispatches an execution that completes and reports a record linked
      // to the job by name — the way a job's runs are discovered.
      eventually {
        val results = Await.result(apiMethods.listBackgroundQueries(defaultNamespaceId, Some(jobName)), 5.seconds)
        assert(results.nonEmpty, "job-dispatched executions appear in the filtered results list")
        assert(results.forall(_.jobName.contains(jobName)))
        assert(results.exists(_.status == "completed"))
      }

      val status = Await
        .result(apiMethods.getJob(jobName), 5.seconds)
        .getOrElse(fail("job should be queryable"))
      assert(status.name == "tick job")
      assert(status.jobType == "background-query")
      status.schedule match {
        case Schedule.Interval(every, startAt) =>
          assert(every == 1.hour)
          assert(startAt.isDefined, "the omitted anchor is resolved to the creation time")
        case other => fail(s"expected an interval schedule, got $other")
      }

      // And listJobs includes the job.
      assert(Await.result(apiMethods.listJobs(), 5.seconds).exists(_.name == jobName))
    } finally {
      val _ = Await.result(apiMethods.deleteJob(jobName), 5.seconds) // stop this job firing into later tests
    }
  }

  test("getJob returns NotFound for an unknown name") {
    assert(Await.result(apiMethods.getJob("no-such-job"), 5.seconds).isLeft)
  }

  test("createJob rejects a duplicate name unless updateIfExists is set") {
    def create(query: String, updateIfExists: Option[Boolean]) =
      Await.result(
        apiMethods.createJob(
          CreateJobRequest(
            schedule = Schedule.Daily(java.time.LocalTime.of(3, 0)), // 03:00 daily, so it won't fire during the test
            action = Action.BackgroundQuery(query = query, destinations = drop),
            name = "dup job",
            updateIfExists = updateIfExists,
          ),
        ),
        5.seconds,
      )
    try {
      assert(create("RETURN 1", updateIfExists = None).isRight, "the first create succeeds")
      assert(create("RETURN 2", updateIfExists = None).isLeft, "a second create of the same name is rejected")
      assert(create("RETURN 3", updateIfExists = Some(true)).isRight, "updateIfExists replaces the existing job")
    } finally {
      val _ = Await.result(apiMethods.deleteJob("dup job"), 5.seconds)
    }
  }

  test("cancelBackgroundQuery cancels an in-flight run; the record transitions to cancelled and is retained") {
    val id = Await
      .result(
        apiMethods.runBackgroundQuery(
          defaultNamespaceId,
          // A long-running query so it is still in flight when the cancel arrives.
          BackgroundQueryDef("UNWIND range(1, 50000000) AS i RETURN i", drop),
        ),
        5.seconds,
      )
      .getOrElse(fail("expected an id"))
    // The Started record is written asynchronously; wait for it so the cancel can find the execution.
    eventually(
      assert(Await.result(apiMethods.getBackgroundQuery(defaultNamespaceId, id), 5.seconds).isRight),
    )
    assert(
      Await.result(apiMethods.cancelBackgroundQuery(defaultNamespaceId, id), 5.seconds).isRight,
      "cancel returns the record",
    )
    eventually(
      assert(
        Await.result(apiMethods.getBackgroundQuery(defaultNamespaceId, id), 5.seconds).exists(_.status == "cancelled"),
        "the record transitions to the terminal cancelled state and remains readable",
      ),
    )
  }

  test("cancelBackgroundQuery on a terminal execution is a no-op that retains the record") {
    val id = Await
      .result(
        apiMethods.runBackgroundQuery(defaultNamespaceId, BackgroundQueryDef("CREATE (:DelOne) RETURN 1", drop)),
        5.seconds,
      )
      .getOrElse(fail("expected an id"))
    eventually(
      assert(
        Await.result(apiMethods.getBackgroundQuery(defaultNamespaceId, id), 5.seconds).exists(_.status == "completed"),
      ),
    )
    assert(
      Await.result(apiMethods.cancelBackgroundQuery(defaultNamespaceId, id), 5.seconds).isRight,
      "cancelling a terminal execution succeeds as a no-op",
    )
    assert(
      Await.result(apiMethods.getBackgroundQuery(defaultNamespaceId, id), 5.seconds).exists(_.status == "completed"),
      "the terminal record is untouched by the no-op cancel",
    )
  }

  test("cancelBackgroundQuery returns NotFound for an unknown id") {
    assert(Await.result(apiMethods.cancelBackgroundQuery(defaultNamespaceId, UUID.randomUUID()), 5.seconds).isLeft)
  }

  test("deleteJob removes the job; its dispatched execution records remain until expiry") {
    val jobName = Await
      .result(
        apiMethods.createJob(
          CreateJobRequest(
            schedule = Schedule.Interval(1.hour, None),
            action = Action.BackgroundQuery(query = "CREATE (:DelJob) RETURN 1", destinations = drop),
            name = "del job",
          ),
        ),
        5.seconds,
      )
      .getOrElse(fail("expected a job name"))
    try {
      eventually {
        val results = Await.result(apiMethods.listBackgroundQueries(defaultNamespaceId, Some(jobName)), 5.seconds)
        assert(results.exists(_.status == "completed"), "the job dispatched a completed execution")
      }
      assert(Await.result(apiMethods.deleteJob(jobName), 5.seconds).isRight, "delete returns the job status")
      assert(Await.result(apiMethods.getJob(jobName), 5.seconds).isLeft, "job removed from the scheduler")
      // Past executions' status records are retained (still queryable by the job's name) and left to
      // expire on their own.
      assert(
        Await
          .result(apiMethods.listBackgroundQueries(defaultNamespaceId, Some(jobName)), 5.seconds)
          .exists(_.status == "completed"),
        "dispatched execution records outlive the deleted job",
      )
    } finally {
      val _ = Await.result(apiMethods.deleteJob(jobName), 5.seconds) // idempotent: the body already deleted it
    }
  }

  test("deleteJob returns NotFound for an unknown name") {
    assert(Await.result(apiMethods.deleteJob("no-such-job"), 5.seconds).isLeft)
  }

  test("an unknown graph returns NotFound (not BadRequest) on writes") {
    val noSuchGraph = com.thatdot.quine.graph.NamespaceId("nosuchgraph")
    // The write methods return a `BadRequest :+: NotFound :+: CNil`; a missing graph must be the
    // NotFound arm, matching the rest of V2.
    def assertNotFound(err: BadRequest :+: NotFound :+: CNil): Unit = err match {
      case Inr(Inl(_: NotFound)) => ()
      case other => fail(s"expected NotFound for an unknown graph, got $other")
    }
    assertNotFound(
      Await
        .result(apiMethods.runBackgroundQuery(noSuchGraph, BackgroundQueryDef("RETURN 1", drop)), 5.seconds)
        .swap
        .getOrElse(fail("run in an unknown graph should be rejected")),
    )
    assertNotFound(
      Await
        .result(
          apiMethods.createJob(
            CreateJobRequest(
              Schedule.Hourly(minute = 0),
              Action.BackgroundQuery(Some(noSuchGraph.name), "RETURN 1", destinations = drop),
              name = "unknown-graph job",
            ),
          ),
          5.seconds,
        )
        .swap
        .getOrElse(fail("job creation targeting an unknown graph should be rejected")),
    )
  }

  test("a malformed action namespace is a BadRequest (not an escaping 500)") {
    // `NamespaceId.apply` throws on names outside `[a-z][a-z0-9]{0,15}`; the create must fold that into
    // a BadRequest rather than let the exception escape as a 500.
    def createErr(namespace: String): BadRequest :+: NotFound :+: CNil =
      Await
        .result(
          apiMethods.createJob(
            CreateJobRequest(
              Schedule.Hourly(minute = 0),
              Action.BackgroundQuery(Some(namespace), "RETURN 1", destinations = drop),
              name = "malformed-namespace job",
            ),
          ),
          5.seconds,
        )
        .swap
        .getOrElse(fail("a malformed namespace should be rejected"))
    def assertBadRequest(err: BadRequest :+: NotFound :+: CNil): Unit = err match {
      case Inl(_: BadRequest) => ()
      case other => fail(s"expected BadRequest for a malformed namespace, got $other")
    }
    // Uppercase, hyphen, leading digit, empty, and >16 chars all fail the pattern.
    Seq("BadName", "bad-name", "1leading", "", "toolongnamespaceid").foreach(n => assertBadRequest(createErr(n)))
  }

  test("validation: an out-of-range schedule field and sub-millisecond statusExpiry are rejected") {
    assert(
      Await
        .result(
          apiMethods.createJob(
            CreateJobRequest(
              Schedule.Hourly(minute = 99), // minute out of range
              Action.BackgroundQuery(query = "RETURN 1", destinations = drop),
              name = "bad-schedule job",
            ),
          ),
          5.seconds,
        )
        .isLeft,
      "a schedule field outside its valid range must be rejected",
    )
    assert(
      Await
        .result(
          apiMethods
            .runBackgroundQuery(
              defaultNamespaceId,
              BackgroundQueryDef("RETURN 1", drop, statusExpiry = Some(500.micros)),
            ),
          5.seconds,
        )
        .isLeft,
      "a sub-ms statusExpiry truncates to 0 (immediate expiry) and must be rejected",
    )
  }

  test("validation: names are trimmed, and empty or control-character names are rejected") {
    def create(name: String) =
      Await.result(
        apiMethods.createJob(
          CreateJobRequest(
            Schedule.Daily(java.time.LocalTime.of(3, 0)), // 03:00 daily, so it won't fire during the test
            Action.BackgroundQuery(query = "RETURN 1", destinations = drop),
            name = name,
          ),
        ),
        5.seconds,
      )
    assert(create("   ").isLeft, "a whitespace-only job name is rejected")
    assert(create("bad\u0000name").isLeft, "a NUL in a job name is rejected")
    assert(create("bad\tname").isLeft, "a tab (control character) inside a job name is rejected")
    assert(
      Await
        .result(
          apiMethods.runBackgroundQuery(
            defaultNamespaceId,
            BackgroundQueryDef("RETURN 1", drop, name = Some("bad\u0007name")),
          ),
          5.seconds,
        )
        .isLeft,
      "a control character in a run-now query's optional name is rejected",
    )
    // A padded name is accepted, but the registered key is the trimmed value.
    try {
      assert(create("  padded job  ").getOrElse(fail("padded name should be accepted")) == "padded job")
      assert(Await.result(apiMethods.getJob("padded job"), 5.seconds).isRight, "the job is keyed by the trimmed name")
    } finally {
      val _ = Await.result(apiMethods.deleteJob("padded job"), 5.seconds)
    }
  }

  test("validation: an uncompilable query and an unsupplied parameter are rejected at accept") {
    assert(
      Await
        .result(apiMethods.runBackgroundQuery(defaultNamespaceId, BackgroundQueryDef("RETRN 1", drop)), 5.seconds)
        .isLeft,
      "a run-now query with a syntax error is rejected",
    )
    assert(
      Await
        .result(
          apiMethods.createJob(
            CreateJobRequest(
              Schedule.Daily(java.time.LocalTime.of(3, 0)),
              Action.BackgroundQuery(query = "MATCH (n RETURN n", destinations = drop),
              name = "bad-query job",
            ),
          ),
          5.seconds,
        )
        .isLeft,
      "a job whose query has a syntax error is rejected at creation, not at first fire",
    )
    assert(
      Await
        .result(
          apiMethods.runBackgroundQuery(defaultNamespaceId, BackgroundQueryDef("RETURN $missing", drop)),
          5.seconds,
        )
        .isLeft,
      "a query referencing a parameter the request does not supply is rejected",
    )
    // Guard the other direction: a valid query using a supplied parameter still passes.
    assert(
      Await
        .result(
          apiMethods.runBackgroundQuery(
            defaultNamespaceId,
            BackgroundQueryDef("RETURN $x AS x", drop, parameters = Map("x" -> io.circe.Json.fromInt(1))),
          ),
          5.seconds,
        )
        .isRight,
      "a query whose parameters are supplied compiles and is accepted",
    )
  }

  test("validation: an unknown timezone and a zero statusExpiry are rejected") {
    val query = "RETURN 1"
    assert(
      Await
        .result(
          apiMethods.createJob(
            CreateJobRequest(
              Schedule.Hourly(minute = 0, timezone = "Not/AZone"),
              Action.BackgroundQuery(query = query, destinations = drop),
              name = "bad-tz job",
            ),
          ),
          5.seconds,
        )
        .isLeft,
      "an unresolvable IANA timezone id must be rejected",
    )
    assert(
      Await
        .result(
          apiMethods.runBackgroundQuery(
            defaultNamespaceId,
            BackgroundQueryDef("RETURN 1", drop, statusExpiry = Some(Duration.Zero)),
          ),
          5.seconds,
        )
        .isLeft,
    )
  }
}

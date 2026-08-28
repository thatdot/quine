package com.thatdot.quine.app

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

import org.apache.pekko.stream.scaladsl.Sink

import cats.data.NonEmptyList
import io.circe.parser
import io.circe.syntax._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.outputs2.kafka.KafkaExtensionProvider
import com.thatdot.quine.app.model.jobs._
import com.thatdot.quine.app.model.outputs2.query.standing.LocalTapBus
import com.thatdot.quine.app.v2api.converters.Api2ToOutputs2
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.compiler.cypher.queryCypherValues
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.graph.scheduledjob.{
  ScheduleSpec,
  ScheduledJobCreateOutcome,
  ScheduledJobDriver,
  ScheduledJobExecutor,
  ScheduledJobState,
}
import com.thatdot.quine.graph.{GraphService, defaultNamespaceId}
import com.thatdot.quine.model.{Milliseconds, QuineIdProvider}
import com.thatdot.quine.serialization.ProtobufSchemaCache

/** Drives the OSS [[JobScheduler]] end to end: jobs fire repeatedly (each fire reporting its own
  * execution record, linked to the job by name), missed slots catch up on restart, interrupted runs
  * re-fire, and the sweep physically deletes expired status records.
  */
class JobSchedulerTest extends AnyFunSuite with BeforeAndAfterAll with Eventually {

  implicit val logConfig: LogConfig = LogConfig.permissive
  private val graph: GraphService = IngestTestGraph.makeGraph("job-scheduler-test")
  implicit val ec: ExecutionContext = graph.nodeDispatcherEC
  implicit val idProvider: QuineIdProvider = graph.idProvider

  private val protobufSchemaCache: ProtobufSchemaCache = new ProtobufSchemaCache.AsyncLoading(graph.dispatchers)
  private val kafkaExtensions: KafkaExtensionProvider[com.thatdot.api.v2.SaslJaasConfig] = KafkaExtensionProvider.empty
  private val drop: NonEmptyList[QuineDestinationSteps] = NonEmptyList.one(QuineDestinationSteps.Drop)

  private val registry = new BackgroundQueryStatusRegistry(graph, hostPresent = _ == "test-host")
  private val runner = new BackgroundQueryRunner(
    graph,
    "test-host",
    registry,
    new LocalTapBus,
    steps => Api2ToOutputs2.apply(steps)(graph, protobufSchemaCache, kafkaExtensions),
  )
  private def newScheduler() = new JobScheduler(graph, JobWork.executorFor(runner), registry)

  private val StateKey = "job_schedule_state"

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(100, Millis))

  override def beforeAll(): Unit =
    while (!graph.isReady) Thread.sleep(10)

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), 10.seconds)

  private def count(label: String): Long = {
    val rows = Await.result(
      queryCypherValues(s"MATCH (n:$label) RETURN count(n)", defaultNamespaceId)(graph).results
        .runWith(Sink.seq)(graph.materializer),
      10.seconds,
    )
    Value.toJson(rows.head.head).asNumber.flatMap(_.toLong).getOrElse(fail("count was not a number"))
  }

  // Fires immediately on creation, then every second — quick, repeated fires for the firing tests.
  private val everySecond = ScheduleSpec.Interval(FiniteDuration(1000, MILLISECONDS), startAt = None)
  // Top of the hour: the seed-driven tests fire from a seeded past `nextFireAt`, and won't re-fire
  // during the test.
  private val hourly = ScheduleSpec.Hourly(minute = 0, zoneId = "UTC")

  private def makeJob(query: String, schedule: ScheduleSpec, name: String = "test job"): Job =
    Job(
      name = name,
      schedule = schedule,
      work = JobWork.RunBackgroundQuery(BackgroundQuery(query, drop)),
    )

  private def seedState(name: String, state: ScheduledJobState): Unit = {
    val seeded: Map[String, ScheduledJobState] = Map(name -> state)
    Await.result(
      graph.namespacePersistor.setMetaData(StateKey, Some(seeded.asJson.noSpaces.getBytes(UTF_8))),
      5.seconds,
    )
  }

  test("a cron job fires repeatedly, each fire reporting its own execution record linked to the job") {
    val scheduler = newScheduler()
    Await.result(scheduler.start(), 5.seconds)
    val job = makeJob("CREATE (:Tick) RETURN 1", everySecond)
    Await.result(scheduler.createJob(job, updateIfExists = false), 5.seconds)

    try {
      eventually(assert(count("Tick") >= 2L, "job should fire at least twice over multiple intervals"))
      // Each dispatched execution reports its own record, discoverable by the job's name.
      eventually {
        val records = Await.result(registry.list(Some(job.name)), 5.seconds)
        assert(records.size >= 2, "per-execution records accumulate, queryable by job name")
      }
    } finally scheduler.stop()
  }

  test("a slot missed while the scheduler was down fires immediately on restart") {
    val now = Milliseconds.currentTime()
    val job = makeJob("CREATE (:CaughtUp) RETURN 1", hourly)
    seedState(
      job.name,
      ScheduledJobState(
        jobType = job.work.jobType,
        payload = job.work.payloadJson,
        schedule = job.schedule,
        nextFireAt = Some(Milliseconds(now.millis - 30000)), // the missed slot
        lastFireAt = Some(Milliseconds(now.millis - 90000)),
      ),
    )

    val scheduler = newScheduler()
    Await.result(scheduler.start(), 5.seconds)
    try eventually(assert(count("CaughtUp") >= 1L, "missed slot should fire immediately on activation"))
    finally scheduler.stop()
  }

  test("a run interrupted mid-execution re-fires on restart as a fresh execution (at-least-once)") {
    val now = Milliseconds.currentTime()
    val job = makeJob("CREATE (:Recovered) RETURN 1", hourly)
    seedState(
      job.name,
      ScheduledJobState(
        jobType = job.work.jobType,
        payload = job.work.payloadJson,
        schedule = job.schedule,
        nextFireAt = Some(Milliseconds(now.millis + 100000)),
        lastFireAt = Some(now),
        inProgressSince = Some(now), // died mid-run
      ),
    )

    val scheduler = newScheduler()
    Await.result(scheduler.start(), 5.seconds)
    try {
      eventually(assert(count("Recovered") >= 1L, "interrupted run should re-fire on restart"))
      // The re-fire is a fresh execution reporting its own record, linked to the job by name.
      eventually {
        val records = Await.result(registry.list(Some(job.name)), 5.seconds)
        assert(records.nonEmpty, "the re-fired execution reports a record under the job's name")
      }
    } finally scheduler.stop()
  }

  test("an undecodable persisted state disables persistence and refuses new jobs (no clobber)") {
    val corrupt = "{ this is not valid scheduled-job json".getBytes(UTF_8)
    Await.result(graph.namespacePersistor.setMetaData(StateKey, Some(corrupt)), 5.seconds)

    val scheduler = newScheduler()
    Await.result(scheduler.start(), 5.seconds) // start completes but marks the scheduler degraded
    try {
      val job = makeJob("CREATE (:NeverRuns) RETURN 1", everySecond)
      assert(
        Await
          .result(scheduler.createJob(job, updateIfExists = false).failed, 5.seconds)
          .isInstanceOf[IllegalStateException],
        "createJob is refused while the load is degraded",
      )
      // The undecodable blob is left intact — a failed load never overwrote it.
      val stored = Await.result(graph.namespacePersistor.getMetaData(StateKey), 5.seconds)
      assert(
        stored.map(new String(_, UTF_8)).contains(new String(corrupt, UTF_8)),
        "the durable state must not be clobbered by a degraded scheduler",
      )
    } finally {
      scheduler.stop()
      Await.result(graph.namespacePersistor.setMetaData(StateKey, None), 5.seconds) // reset for other tests
    }
  }

  test("the driver's wake runs the manager sweep, deleting a departed host's expired status record") {
    val now = Milliseconds.currentTime()
    // nextFireAt is seeded far in the future below, so this job won't fire during the test.
    val job = makeJob("RETURN 1", hourly)
    val expiredId = java.util.UUID.randomUUID()
    // Seed a job, and an already-expired record owned by a host no longer in the cluster (the
    // manager sweep's scope — a present host's records are its own owner sweep's).
    seedState(
      job.name,
      ScheduledJobState(
        jobType = job.work.jobType,
        payload = job.work.payloadJson,
        schedule = job.schedule,
        nextFireAt = Some(Milliseconds(now.millis + 500000)),
      ),
    )
    Await.result(
      registry.put(
        BackgroundQueryRecord(
          executionId = expiredId,
          jobName = Some(job.name),
          namespace = defaultNamespaceId.name,
          hostId = "dead-host",
          name = None,
          query = "RETURN 1",
          lastAction = ExecutionAction.Failed("stale"),
          expiresAtMillis = now.millis - 1000, // already expired
        ),
      ),
      5.seconds,
    )

    val scheduler = newScheduler()
    Await.result(scheduler.start(), 5.seconds) // start arms + wakes; the wake sweeps
    try eventually {
      val stored = Await.result(
        graph.namespacePersistor.getMetaData(BackgroundQueryStatusRegistry.KeyPrefix + expiredId),
        5.seconds,
      )
      assert(stored.isEmpty, "the manager sweep physically deletes the departed host's expired record")
    } finally scheduler.stop()
  }

  test("an interrupted run is deferred, not dropped, when no executor is registered yet") {
    val now = Milliseconds.currentTime()
    val job = makeJob("CREATE (:Deferred) RETURN 1", hourly, name = "deferred job")
    seedState(
      job.name,
      ScheduledJobState(
        jobType = job.work.jobType,
        payload = job.work.payloadJson,
        schedule = job.schedule,
        nextFireAt = Some(Milliseconds(now.millis + 100000)), // far off: only the re-fire can run it
        lastFireAt = Some(now),
        inProgressSince = Some(now), // died mid-run
      ),
    )

    // The enterprise app registers its executor after cluster construction, so a load can resolve
    // before one exists. Model that with a supplier that starts empty.
    val executor = new AtomicReference[Option[ScheduledJobExecutor]](None)
    val driver = new ScheduledJobDriver(graph, StateKey, () => executor.get(), () => None)
    Await.result(driver.load(), 5.seconds)
    try {
      // The run is neither dispatched nor abandoned: the marker is kept, which is what holds the
      // job for the re-fire (and out of `dueJobs`) until an executor appears.
      Thread.sleep(500)
      assert(count("Deferred") == 0L, "nothing runs without an executor")
      val loaded = Await.result(driver.getJobs, 5.seconds).getOrElse(job.name, fail("job should be loaded"))
      assert(loaded.inProgressSince.isDefined, "the in-progress marker is kept for the deferred re-fire")

      // Once an executor is registered, the next wake re-fires the interrupted run. Creating an
      // immediately-due job re-arms the timer, so that wake comes now rather than at the driver's cap.
      executor.set(Some(JobWork.executorFor(runner)))
      val waker = makeJob("RETURN 1", everySecond, name = "waker job")
      Await.result(
        driver.createJob(
          waker.name,
          waker.work.jobType,
          waker.work.payloadJson,
          waker.schedule,
          updateIfExists = true,
          requestId = java.util.UUID.randomUUID().toString,
        ),
        5.seconds,
      )
      eventually(assert(count("Deferred") >= 1L, "the deferred re-fire runs once an executor is registered"))
    } finally {
      driver.stop()
      Await.result(graph.namespacePersistor.setMetaData(StateKey, None), 5.seconds)
    }
  }

  test("create is idempotent under retry: the same requestId returns Created, a different one AlreadyExists") {
    val driver = new ScheduledJobDriver(graph, StateKey, () => None, () => None)
    Await.result(driver.load(), 5.seconds)
    try {
      val job = makeJob("RETURN 1", hourly, name = "idem job") // hourly: won't fire during the test
      def create(updateIfExists: Boolean, requestId: String): ScheduledJobCreateOutcome =
        Await.result(
          driver.createJob(job.name, job.work.jobType, job.work.payloadJson, job.schedule, updateIfExists, requestId),
          5.seconds,
        )

      val reqId = java.util.UUID.randomUUID().toString
      assert(create(updateIfExists = false, reqId) == ScheduledJobCreateOutcome.Created, "the first create succeeds")
      // Re-issuing the same logical request (its slow first ack was lost, so the caller retried) is
      // recognized as already-applied — Created, not a spurious AlreadyExists.
      assert(
        create(updateIfExists = false, reqId) == ScheduledJobCreateOutcome.Created,
        "a retry with the same requestId returns Created, not AlreadyExists",
      )
      // A genuinely different request for the same name still conflicts.
      assert(
        create(updateIfExists = false, java.util.UUID.randomUUID().toString) == ScheduledJobCreateOutcome.AlreadyExists,
        "a different request for an existing name is rejected",
      )
    } finally {
      driver.stop()
      Await.result(graph.namespacePersistor.setMetaData(StateKey, None), 5.seconds)
    }
  }

  test("completion enqueued after stop() is dropped, not persisted over the durable blob") {
    // An executor whose run stays pending until we complete it, so we control when the completion
    // (which enqueues a write) happens relative to stop().
    val running = Promise[Unit]()
    val launched = Promise[Unit]()
    val executor = new ScheduledJobExecutor {
      override def execute(jobName: String, executionId: String, jobType: String, payload: String): Future[Unit] = {
        launched.trySuccess(())
        running.future
      }
    }
    def persisted(): Map[String, ScheduledJobState] =
      Await
        .result(graph.namespacePersistor.getMetaData(StateKey), 5.seconds)
        .flatMap(b => parser.decode[Map[String, ScheduledJobState]](new String(b, UTF_8)).toOption)
        .getOrElse(Map.empty)

    val driver = new ScheduledJobDriver(graph, StateKey, () => Some(executor), () => None)
    Await.result(driver.load(), 5.seconds)
    try {
      // An immediately-due job fires, launches (executor run now pending), and is marked in-progress.
      Await.result(
        driver
          .createJob("stopjob", "bg", "p", everySecond, updateIfExists = false, java.util.UUID.randomUUID().toString),
        5.seconds,
      )
      Await.result(launched.future, 5.seconds)
      eventually(assert(persisted().get("stopjob").exists(_.inProgressSince.isDefined), "fired job is in-progress"))
      val before = persisted()

      // Depose this driver, THEN let the run finish — its completion enqueues onto a stopped driver.
      driver.stop()
      running.trySuccess(())
      Thread.sleep(500) // give the drained completion a chance to (wrongly) persist

      assert(persisted() == before, "a completion after stop() must not persist over the durable blob")
      assert(
        persisted().get("stopjob").exists(_.inProgressSince.isDefined),
        "the in-progress marker is left as-is (the post-stop completion was dropped, not written)",
      )
    } finally {
      driver.stop()
      Await.result(graph.namespacePersistor.setMetaData(StateKey, None), 5.seconds)
    }
  }

  test("a failed load is not permanent — it retries and recovers to Ready") {
    // Seed an undecodable blob so the first load fails.
    Await.result(graph.namespacePersistor.setMetaData(StateKey, Some("not json".getBytes(UTF_8))), 5.seconds)
    val driver = new ScheduledJobDriver(graph, StateKey, () => None, () => None, loadRetryDelay = 500.millis)
    try {
      Await.result(driver.load(), 5.seconds)
      // While Failed, operations are rejected.
      val rejected =
        driver.createJob("j", "bg", "p", hourly, updateIfExists = false, java.util.UUID.randomUUID().toString)
      assert(Await.result(rejected.failed, 5.seconds).isInstanceOf[IllegalStateException], "ops rejected while failed")

      // Repair the durable state; the scheduled retry re-loads and reaches Ready.
      Await.result(graph.namespacePersistor.setMetaData(StateKey, None), 5.seconds)
      eventually {
        val outcome = Await.result(
          driver.createJob("j2", "bg", "p", hourly, updateIfExists = false, java.util.UUID.randomUUID().toString),
          5.seconds,
        )
        assert(
          outcome == ScheduledJobCreateOutcome.Created,
          "after the load retry, the driver is Ready and accepts creates",
        )
      }
    } finally {
      driver.stop()
      Await.result(graph.namespacePersistor.setMetaData(StateKey, None), 5.seconds)
    }
  }
}

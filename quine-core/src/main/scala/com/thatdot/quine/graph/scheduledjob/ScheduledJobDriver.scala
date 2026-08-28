package com.thatdot.quine.graph.scheduledjob

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

import org.apache.pekko.actor.Cancellable

import io.circe.syntax._
import io.circe.{Decoder, Encoder, parser}

import com.thatdot.common.logging.Log.{LazySafeLogging, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.model.Milliseconds

/** The scheduled-job driver: owns the job registry's run-state, persistence, the dynamic-deadline
  * timer, and dispatch. One implementation, shared by the OSS single-host scheduler and the
  * enterprise elected-manager protocol; the scheduling math lives in the pure [[JobSchedule]].
  *
  * Consistency: all durable writes flow through one serialized queue (one persist in flight at a
  * time), so persisted blobs form a single ordered history. Acked operations (create/delete) are
  * persist-then-commit — an ack always means "durably recorded", and a failed persist changes
  * nothing (no rollback, so no rollback race). Progress operations (fire/completion) are
  * commit-now-persist-behind — memory leads so a job cannot double-fire while its persist is in
  * flight, and a lost write is covered by at-least-once re-fire on the next load. Reads serve the
  * committed state.
  *
  * Lifecycle: [[load]] rehydrates from the persistor, buffering operations that arrive first; a
  * failed load rejects everything and never persists, so it cannot clobber the durable blob, and it
  * retries after [[loadRetryDelay]] so a transient persistor error does not disable scheduling for
  * the whole term. On load, runs interrupted by a crash/failover re-fire (at-least-once). A one-shot timer arms to the
  * earliest deadline via [[JobSchedule.armDelay]] (capped at [[maxCap]] so a missed re-arm or clock
  * jump self-corrects); each wake fires due jobs and runs the sweeper (throttled to
  * [[ScheduledJobDriver.SweepInterval]]). `executor`/`sweeper` are suppliers because the enterprise
  * app registers them after cluster construction; both due fires and interrupted-run re-fires are
  * deferred (without advancing schedules, and keeping the in-progress markers) until an executor
  * appears.
  */
class ScheduledJobDriver(
  graph: BaseGraph,
  metadataKey: String,
  executor: () => Option[ScheduledJobExecutor],
  sweeper: () => Option[ScheduledJobSweeper],
  loadRetryDelay: FiniteDuration = 30.seconds,
) extends LazySafeLogging {

  import ScheduledJobDriver._

  implicit private val ec: ExecutionContext = graph.system.dispatcher
  private val maxCap: FiniteDuration = 5.minutes

  implicit private val mapEncoder: Encoder[Map[String, ScheduledJobState]] =
    Encoder.encodeMap[String, ScheduledJobState]
  implicit private val mapDecoder: Decoder[Map[String, ScheduledJobState]] =
    Decoder.decodeMap[String, ScheduledJobState]

  /** Guards all mutable state below. Critical sections are short and never do I/O; persists and
    * launches run outside it.
    */
  private val lock = new Object
  private var phase: LoadPhase = LoadPhase.Loading
  private var pending: Vector[Pending] = Vector.empty
  private var committed: Map[String, ScheduledJobState] = Map.empty
  private var timer: Option[Cancellable] = None
  private var writeBusy: Boolean = false
  private val writeQueue = mutable.Queue.empty[QueuedWrite]

  /** Jobs whose interrupted run is owed a re-fire but had no executor to run it when the load
    * resolved. Their in-progress markers stay set — which holds them out of [[JobSchedule.dueJobs]],
    * so nothing else can fire them meanwhile — and the next wake with an executor dispatches them.
    */
  private var owedRefire: Set[String] = Set.empty

  /** Epoch millis of the last sweep, so the sweeper runs on its own cadence rather than once per
    * wake (wake frequency follows fire frequency; a sweep is a whole-registry pass).
    */
  private var lastSweepAtMillis: Long = 0L

  // ── Public operations ──────────────────────────────────────────────────────────────────────────

  /** Register a scheduled job, keyed by `name`; the first owed slot comes from `firstFireAt`. For
    * an existing name, `updateIfExists = false` leaves it untouched (returning `AlreadyExists`);
    * `true` replaces its definition and recomputes the next fire, preserving run history (last
    * fire, in-flight marker). The returned Future completes only once durably recorded.
    *
    * `requestId` makes the call idempotent under at-least-once retry: it is stamped on the job when
    * this request creates or replaces it, and a later call for the same name carrying the same
    * `requestId` is recognized as that same request having already applied. It then returns success
    * without rewriting — so a first attempt whose slow persist made the caller's ask time out, then
    * retry, does not come back as a spurious `AlreadyExists` (nor re-anchor a deferred schedule).
    * Distinct requests get distinct ids, so a genuine name collision still returns `AlreadyExists`.
    */
  def createJob(
    name: String,
    jobType: String,
    payload: String,
    schedule: ScheduleSpec,
    updateIfExists: Boolean,
    requestId: String,
  ): Future[ScheduledJobCreateOutcome] = {
    val promise = Promise[ScheduledJobCreateOutcome]()
    whenReady(promise) { () =>
      enqueue(
        QueuedWrite(
          compute = { registry =>
            registry.get(name) match {
              case Some(existing) if existing.createRequestId.contains(requestId) =>
                // This exact request already applied (an earlier attempt committed before its ack got
                // back). Return success without rewriting — no re-anchor, no second write. Reports
                // `Created`; the API collapses Created/Updated to success, so the distinction is moot.
                WriteAction.NoChange(() => promise.success(ScheduledJobCreateOutcome.Created))
              case Some(_) if !updateIfExists =>
                WriteAction.NoChange(() => promise.success(ScheduledJobCreateOutcome.AlreadyExists))
              case existing =>
                val now = Milliseconds.currentTime()
                // Resolve any deferred anchor (an Interval with no explicit startAt) to `now` so the
                // persisted schedule is fully concrete and its fire times are stable across restarts.
                val schedule2 = schedule.anchoredAt(now)
                val base = ScheduledJobState(
                  jobType,
                  payload,
                  schedule2,
                  nextFireAt = schedule2.firstFireAt(now),
                  createRequestId = Some(requestId),
                )
                // On update, keep run history + in-flight marker; only the definition, next fire time,
                // and creating-request id change.
                val newState = existing.fold(base) { old =>
                  base.copy(lastFireAt = old.lastFireAt, inProgressSince = old.inProgressSince)
                }
                val outcome =
                  if (existing.isDefined) ScheduledJobCreateOutcome.Updated else ScheduledJobCreateOutcome.Created
                WriteAction.CommitAfterPersist(
                  registry.updated(name, newState),
                  done = () => promise.success(outcome),
                  fail = e => promise.failure(e),
                )
            }
          },
          fail = e => { val _ = promise.tryFailure(e) },
        ),
      )
    }
    promise.future
  }

  /** The committed (acked) registry state. */
  def getJobs: Future[Map[String, ScheduledJobState]] = {
    val promise = Promise[Map[String, ScheduledJobState]]()
    whenReady(promise)(() => promise.success(lock.synchronized(committed)))
    promise.future
  }

  /** Remove a job and erase its persisted entry, returning its last state — or `None` if no such
    * job. The returned Future completes only once the removal is durable.
    */
  def deleteJob(name: String): Future[Option[ScheduledJobState]] = {
    val promise = Promise[Option[ScheduledJobState]]()
    whenReady(promise) { () =>
      enqueue(
        QueuedWrite(
          compute = { registry =>
            registry.get(name) match {
              case None => WriteAction.NoChange(() => promise.success(None))
              case Some(st) =>
                WriteAction.CommitAfterPersist(
                  registry - name,
                  done = () => promise.success(Some(st)),
                  fail = e => promise.failure(e),
                )
            }
          },
          fail = e => { val _ = promise.tryFailure(e) },
        ),
      )
    }
    promise.future
  }

  /** Rehydrate from the persistor, re-fire interrupted runs, arm the timer, run an activation sweep,
    * and replay operations buffered while loading. The returned Future completes when the load has
    * resolved either way (a failed load logs, rejects buffered operations, and schedules a retry via
    * [[scheduleLoadRetry]] — it does not fail the Future, matching app-startup semantics).
    */
  def load(): Future[Unit] =
    graph.namespacePersistor
      .getMetaData(metadataKey)
      .map {
        case None => onLoaded(Map.empty) // no prior state — clean start
        case Some(bytes) =>
          parser.decode(new String(bytes, UTF_8))(mapDecoder) match {
            case Right(states) => onLoaded(states)
            case Left(err) => onLoadFailed(err.getMessage)
          }
      }
      .recover { case e => onLoadFailed(e.getMessage) }

  /** Stop the driver (shutdown / losing the manager election): mark it `Stopped` and cancel the
    * timer. Sticky — after this a racing operation is rejected, an in-flight `wake` becomes a no-op,
    * and a `load` that resolves afterwards stays stopped instead of reviving. A write still queued or
    * enqueued afterwards (e.g. a run completing late) is drained by [[pump]] WITHOUT persisting, so a
    * deposed driver cannot clobber a successor manager's blob; the one persist that was already in
    * flight when `stop` landed still drains. Any owed re-fire is dropped: the next load recomputes
    * what was interrupted from the durable markers.
    */
  def stop(): Unit = lock.synchronized {
    phase = LoadPhase.Stopped
    timer.foreach(_.cancel())
    timer = None
    owedRefire = Set.empty
  }

  // ── Load lifecycle ─────────────────────────────────────────────────────────────────────────────

  private def onLoaded(states: Map[String, ScheduledJobState]): Unit = {
    // If stop() landed during the load, stay stopped instead of reviving to Ready — reject anything
    // that was buffered while loading rather than running it against a driver that is shutting down.
    val outcome: Either[Vector[Pending], Vector[Pending]] = lock.synchronized {
      val p = pending
      pending = Vector.empty
      if (phase == LoadPhase.Stopped) Left(p)
      else {
        committed = states
        phase = LoadPhase.Ready
        armLocked()
        Right(p)
      }
    }
    outcome match {
      case Left(rejected) => rejected.foreach(_.reject())
      case Right(toRun) =>
        logger.info(safe"Loaded ${Safe(states.size.toString)} scheduled job(s) from persistence")
        refireInterrupted()
        sweep() // activation sweep: results may have expired while no driver was active
        toRun.foreach(_.run())
    }
  }

  private def onLoadFailed(message: String): Unit = {
    val toReject = lock.synchronized {
      phase = LoadPhase.Failed
      val p = pending
      pending = Vector.empty
      p
    }
    logger.error(
      safe"Failed to load scheduled jobs; will retry in ${Safe(loadRetryDelay.toString)}: ${Safe(message)}",
    )
    toReject.foreach(_.reject())
    scheduleLoadRetry()
  }

  /** A failed load is transient, not terminal: after [[loadRetryDelay]], if the driver is still
    * `Failed` (a [[stop]] would have moved it to `Stopped`, which must not revive), re-enter `Loading`
    * and load again. So one persistor blip at election time does not disable scheduling for the whole
    * term. The load is a pure read, so retrying cannot clobber the durable blob.
    */
  private def scheduleLoadRetry(): Unit = {
    val _ = graph.system.scheduler.scheduleOnce(loadRetryDelay) {
      val retry = lock.synchronized {
        if (phase == LoadPhase.Failed) { phase = LoadPhase.Loading; true }
        else false
      }
      if (retry) { val _ = load() }
    }
  }

  private def loadFailedError: IllegalStateException =
    new IllegalStateException("Scheduled-job registry failed to load; refusing scheduled-job requests")

  private def stoppedError: IllegalStateException =
    new IllegalStateException("Scheduled-job driver stopped; refusing scheduled-job requests")

  /** Run `op` if the registry is Ready; buffer it during Loading (replayed on load, rejected on a
    * failed load); reject immediately after a failed load or once stopped. `op` runs outside the lock.
    */
  private def whenReady(promise: Promise[_])(op: () => Unit): Unit = {
    val decision: Int = lock.synchronized {
      phase match {
        case LoadPhase.Ready => RunNow
        case LoadPhase.Loading =>
          pending :+= Pending(run = op, reject = () => promise.failure(loadFailedError))
          Buffered
        case LoadPhase.Failed => Reject
        case LoadPhase.Stopped => RejectStopped
      }
    }
    decision match {
      case RunNow => op()
      case Reject => val _ = promise.failure(loadFailedError)
      case RejectStopped => val _ = promise.failure(stoppedError)
      case _ => ()
    }
  }

  // ── Serialized write queue ─────────────────────────────────────────────────────────────────────

  private def enqueue(w: QueuedWrite): Unit = {
    lock.synchronized(writeQueue.enqueue(w))
    pump()
  }

  /** Advance the write queue: run the head write's `compute` against the committed registry (under
    * the lock), then execute its action. At most one persist is in flight at a time, so persisted
    * blobs form a total order and a slow write can never overwrite a later one with a stale
    * snapshot.
    */
  private def pump(): Unit = {
    // Right(action) to run it; Left((fail, error)) if the head write's `compute` threw; None if idle.
    val next: Option[Either[(Throwable => Unit, Throwable), WriteAction]] = lock.synchronized {
      if (writeBusy || writeQueue.isEmpty) None
      else if (phase != LoadPhase.Ready) {
        // Not Ready — Stopped (deposed / shut down) or Failed (load disabled): drain the queued write
        // WITHOUT persisting. A late completion enqueued by a now-stale driver (e.g. a run finishing
        // after this host lost the manager election) must never persist its snapshot over a successor
        // manager's blob. armLocked is already inert when not Ready, so nothing here re-arms either.
        val w = writeQueue.dequeue()
        val err = if (phase == LoadPhase.Stopped) stoppedError else loadFailedError
        Some(Left((w.fail, err)))
      } else {
        val w = writeQueue.dequeue()
        Try(w.compute(committed)) match {
          case Success(action) =>
            action match {
              case WriteAction.NoChange(_) => () // completes below; no persist, queue not busied
              case WriteAction.CommitNowPersistBehind(candidate, _) =>
                committed = candidate // memory leads (see the consistency model above)
                armLocked()
                writeBusy = true
              case WriteAction.CommitAfterPersist(_, _, _) =>
                writeBusy = true
            }
            Some(Right(action))
          case Failure(e) =>
            // A `compute` closure threw (e.g. schedule math). Keep the serialized driver alive: the
            // write is already dequeued and `writeBusy` was never set, so re-arm the timer to the
            // committed state (a fire that threw must not leave the one-shot timer dead) and surface
            // the failure below.
            Try(armLocked())
            Some(Left((w.fail, e)))
        }
      }
    }
    next.foreach {
      case Left((fail, e)) =>
        logger.warn(safe"Skipping scheduled-job write: ${Safe(e.getMessage)}")
        fail(e)
        pump()
      case Right(WriteAction.NoChange(done)) =>
        done()
        pump()
      case Right(WriteAction.CommitNowPersistBehind(candidate, effects)) =>
        effects.foreach(_.apply()) // e.g. launches — outside the lock, before the persist resolves
        persist(candidate).onComplete { result =>
          result.failed.foreach { e =>
            // Tolerated under at-least-once: the next serialized persist carries the state forward,
            // and a crash before then re-fires interrupted runs on the next load.
            logger.warn(safe"Best-effort scheduled-job persist failed: ${Safe(e.getMessage)}")
          }
          lock.synchronized { writeBusy = false }
          pump()
        }
      case Right(WriteAction.CommitAfterPersist(candidate, done, fail)) =>
        persist(candidate).onComplete { result =>
          lock.synchronized {
            if (result.isSuccess) {
              committed = candidate
              armLocked()
            }
            writeBusy = false
          }
          result match {
            case Success(_) => done()
            case Failure(e) => fail(e)
          }
          pump()
        }
    }
  }

  private def persist(registry: Map[String, ScheduledJobState]): Future[Unit] =
    graph.namespacePersistor.setMetaData(metadataKey, Some(registry.asJson.noSpaces.getBytes(UTF_8)))

  // ── Timer / fire / sweep ───────────────────────────────────────────────────────────────────────

  /** (Re-)arm the one-shot timer to the earliest deadline (clamp owned by [[JobSchedule.armDelay]]).
    * Must be called with the lock held.
    */
  private def armLocked(): Unit = {
    timer.foreach(_.cancel())
    if (phase == LoadPhase.Ready)
      timer = Some(
        graph.system.scheduler
          .scheduleOnce(JobSchedule.armDelay(committed, Milliseconds.currentTime(), maxCap))(wake()),
      )
  }

  private def armAtLocked(delay: FiniteDuration): Unit = {
    timer.foreach(_.cancel())
    timer = Some(graph.system.scheduler.scheduleOnce(delay)(wake()))
  }

  /** Timer wake: dispatch any owed re-fire, fire due jobs (advancing schedules, minting execution
    * ids) and run the sweeper. A wake that fires after [[stop]] (a one-shot already dispatched when
    * the timer was cancelled) is a no-op — like [[armLocked]], it acts only while `Ready`.
    */
  private def wake(): Unit =
    if (lock.synchronized(phase == LoadPhase.Ready)) {
      retryOwedRefire()
      enqueue(QueuedWrite { registry =>
        val now = Milliseconds.currentTime()
        val due = JobSchedule.dueJobs(registry, now)
        if (due.isEmpty)
          WriteAction.NoChange(() => lock.synchronized(armLocked()))
        else if (executor().isEmpty) {
          // No executor yet (startup race): defer without advancing/marking. Re-arm at the cap, not
          // the computed deadline — the deferred deadlines are in the past and would spin a 0ms timer.
          logger.warn(safe"No scheduled-job executor registered; deferring ${Safe(due.size.toString)} due job(s)")
          WriteAction.NoChange(() => lock.synchronized(armAtLocked(maxCap)))
        } else
          // Fire: advance each due job's schedule as part of the dispatch write.
          dispatchAll(registry, due, advance = JobSchedule.onFire(_, now))
      })
      sweep()
    }

  /** Re-fire runs interrupted by a crashed/failed-over driver (at-least-once), as fresh executions
    * without advancing their schedules (they already advanced on the original fire). Runs on load.
    */
  private def refireInterrupted(): Unit =
    enqueue(QueuedWrite { registry =>
      val interrupted = JobSchedule.interrupted(registry)
      if (interrupted.isEmpty) WriteAction.NoChange(() => ())
      else if (executor().isEmpty) {
        // No executor yet (the enterprise app registers one after cluster construction): defer
        // rather than drop the run. The in-progress markers stay set — which also keeps `dueJobs`
        // from firing these jobs in the meantime — and the owed set is dispatched by the first wake
        // that finds an executor. Re-arm so such a wake is guaranteed to come: the marked jobs are
        // not deadlines, so this is the next real one, or the cap.
        logger.warn(
          safe"No scheduled-job executor registered; deferring re-fire of ${Safe(interrupted.size.toString)} interrupted job(s)",
        )
        lock.synchronized { owedRefire = interrupted }
        WriteAction.NoChange(() => lock.synchronized(armLocked()))
      } else
        // Re-fire: the schedule already advanced on the original fire, so dispatch without advancing.
        dispatchAll(registry, interrupted, advance = identity)
    })

  /** Dispatch a re-fire deferred by [[refireInterrupted]] for want of an executor. The owed set is
    * claimed under the lock, so it is dispatched exactly once; jobs deleted in the meantime are
    * skipped by [[dispatchAll]].
    */
  private def retryOwedRefire(): Unit = {
    val owed = lock.synchronized {
      if (owedRefire.isEmpty || executor().isEmpty) Set.empty[String]
      else {
        val claimed = owedRefire
        owedRefire = Set.empty
        claimed
      }
    }
    if (owed.nonEmpty)
      enqueue(QueuedWrite { registry =>
        val present = owed.filter(registry.contains)
        if (present.isEmpty) WriteAction.NoChange(() => ())
        else dispatchAll(registry, present, advance = identity)
      })
  }

  /** Dispatch every job in `jobNames`: apply `advance` to its state, mint a fresh execution id, and
    * launch — as one progress write committing the whole batch. Shared by the fire path (which
    * advances schedules) and the re-fire path (which does not).
    */
  private def dispatchAll(
    registry: Map[String, ScheduledJobState],
    jobNames: Set[String],
    advance: ScheduledJobState => ScheduledJobState,
  ): WriteAction = {
    var launches = List.empty[() => Unit]
    val next = jobNames.foldLeft(registry) { (r, jobName) =>
      r.get(jobName).fold(r) { st =>
        val executionId = UUID.randomUUID().toString
        launches ::= (() => launch(jobName, executionId, st.jobType, st.payload))
        r.updated(jobName, advance(st))
      }
    }
    WriteAction.CommitNowPersistBehind(next, launches)
  }

  private def launch(jobName: String, executionId: String, jobType: String, payload: String): Unit =
    executor() match {
      case Some(ex) =>
        // Completion (success or failure) clears the in-progress marker via the write queue.
        ex.execute(jobName, executionId, jobType, payload).onComplete(_ => complete(jobName))
      case None =>
        // Defensive, and currently unreachable: every dispatch path checks `executor().isDefined`
        // under the lock before producing a launch, and the executor supplier is a monotonic
        // None -> Some latch (set once at registration, never revoked), so a launch that got queued
        // always still sees `Some` here. Terminal by choice: if a future change ever makes the
        // executor revocable and this does fire, we clear the in-progress marker and let the
        // schedule advance rather than wedge the job in-progress forever — dropping this one run,
        // not re-queuing it to `owedRefire`.
        logger.warn(safe"No scheduled-job executor registered; cannot run ${Safe(jobName)}")
        complete(jobName)
    }

  private def complete(jobName: String): Unit =
    enqueue(QueuedWrite { registry =>
      registry.get(jobName) match {
        case None => WriteAction.NoChange(() => ()) // deleted while running
        case Some(st) =>
          WriteAction.CommitNowPersistBehind(registry.updated(jobName, JobSchedule.onCompletion(st)), Nil)
      }
    })

  /** Clear expired execution status records via the app-registered sweeper, at most once per
    * [[SweepInterval]]. Throttled independently of the timer because a wake happens per fire while a
    * sweep is a whole-registry pass; expiry grants days of grace, so a lazy cadence costs nothing.
    */
  private def sweep(): Unit = {
    val now = Milliseconds.currentTime()
    val due = lock.synchronized {
      if (now.millis - lastSweepAtMillis < SweepInterval.toMillis) false
      else {
        lastSweepAtMillis = now.millis
        true
      }
    }
    if (due)
      sweeper().foreach { sw =>
        sw.sweepExpired(now).onComplete {
          case Success(_) => ()
          case Failure(e) => logger.warn(safe"Execution status record sweep failed: ${Safe(e.getMessage)}")
        }
      }
  }
}

object ScheduledJobDriver {

  /** Cadence of the driver's expired-record sweep — the floor between two sweeps, however often the
    * timer wakes.
    */
  val SweepInterval: FiniteDuration = 10.minutes

  /** Load lifecycle of the driver-owned registry. */
  sealed private trait LoadPhase
  private object LoadPhase {
    case object Loading extends LoadPhase // before load resolves: buffer operations
    case object Ready extends LoadPhase // registry rehydrated; process normally
    case object Failed extends LoadPhase // load failed: reject operations, never persist
    case object Stopped extends LoadPhase // stopped (shutdown / lost election): reject, never revive
  }

  /** An operation buffered while `Loading`: `run` on load, `reject` on a failed load. */
  final private case class Pending(run: () => Unit, reject: () => Unit)

  // whenReady decisions (resolved under the lock, acted on outside it)
  private val RunNow = 0
  private val Buffered = 1
  private val Reject = 2
  private val RejectStopped = 3

  /** One serialized durable write. `compute` runs against the latest committed registry when the
    * write reaches the head of the queue and picks a [[WriteAction]]. If `compute` throws (e.g. the
    * schedule math on a pathological schedule), the driver skips the write and invokes `fail` — for
    * an acked operation this fails its Future instead of hanging the caller; a fire-and-forget write
    * leaves it a no-op (the skip is logged, and the timer is re-armed regardless).
    */
  final private case class QueuedWrite(
    compute: Map[String, ScheduledJobState] => WriteAction,
    fail: Throwable => Unit = _ => (),
  )

  sealed private trait WriteAction
  private object WriteAction {

    /** Nothing to persist: resolve the operation immediately. */
    final case class NoChange(done: () => Unit) extends WriteAction

    /** Acked write (create/delete): persist the candidate and only on success commit it and run
      * `done`; on failure the committed registry is untouched and `fail` runs — no rollback exists.
      */
    final case class CommitAfterPersist(
      candidate: Map[String, ScheduledJobState],
      done: () => Unit,
      fail: Throwable => Unit,
    ) extends WriteAction

    /** Progress write (fire/completion/sweep-strip): commit immediately, run `effects` (launches)
      * outside the lock, persist behind; a failed persist is logged and tolerated (at-least-once).
      */
    final case class CommitNowPersistBehind(
      candidate: Map[String, ScheduledJobState],
      effects: List[() => Unit],
    ) extends WriteAction
  }
}

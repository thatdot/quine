package com.thatdot.quine.app.model.jobs

import java.util.UUID

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.{KillSwitches, SharedKillSwitch}

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.outputs2.DataFoldableSink
import com.thatdot.quine.app.model.outputs2.query.standing.StandingQueryResultWorkflow.queryContextFoldableFrom
import com.thatdot.quine.app.model.outputs2.query.standing.TapBus
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.compiler.cypher.queryCypherValues
import com.thatdot.quine.graph.cypher.{QueryContext, Value}
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId, defaultNamespaceId}
import com.thatdot.quine.model.Milliseconds

/** Runs one execution of a [[BackgroundQuery]] locally: result rows stream to the configured
  * destinations (never persisted) and to a best-effort [[BackgroundQueryTapRelay]] wiretap; only
  * the lifecycle (Started → Completed/Failed/Cancelled) is recorded in the status registry under
  * the execution's id.
  *
  * Cluster-agnostic (needs only `graph.cypherOps`/`graph.materializer`). `hostId` identifies the
  * executing host in the record; `destinationSink` is injected so the app supplies the converter's
  * dependencies (protobuf cache, kafka extensions) without a field-init-order hazard.
  */
class BackgroundQueryRunner(
  graph: CypherOpsGraph,
  hostId: String,
  registry: BackgroundQueryStatusRegistry,
  tapBus: TapBus,
  destinationSink: QuineDestinationSteps => Future[DataFoldableSink],
)(implicit val logConfig: LogConfig)
    extends LazySafeLogging {

  import BackgroundQueryRunner.Execution

  implicit private val ec: ExecutionContext = graph.system.dispatcher

  /** In-flight executions on this host, keyed by execution id — the cancel target and the owner
    * sweep's "is this run alive?" answer. A [[SharedKillSwitch]] so a cancel that lands before the
    * stream materializes is still honored.
    */
  private val inFlight = TrieMap.empty[UUID, SharedKillSwitch]

  // Startup reconciliation, then the periodic owner sweep. Both read this host's records:
  //   - Reconciliation runs once, now, while `inFlight` is empty: any of this host's own `Started`
  //     records is a run that died with a previous incarnation, so finalize it to Interrupted rather
  //     than let it linger as a phantom "running" row until expiry. Safe to do without an expiry gate
  //     precisely because nothing is finishing yet (unlike the periodic sweep below).
  //   - Owner sweep periodically deletes this host's own expired records, never one whose run is
  //     still in `inFlight` — a live run's record survives indefinitely, with no heartbeat writes.
  // Departed hosts' records are the manager sweep's job (see BackgroundQueryStatusRegistry).
  locally {
    registry
      .reconcileOwnedStarted(hostId, inFlight.contains)
      .onComplete {
        case Success(_) => ()
        case Failure(e) =>
          logger.warn(safe"Background-query startup reconciliation failed on ${Safe(hostId)}: ${Safe(e.getMessage)}")
      }(ec)

    val interval = BackgroundQueryRunner.OwnerSweepInterval
    val _ = graph.system.scheduler.scheduleWithFixedDelay(interval, interval) { () =>
      registry.sweepOwnedBy(hostId, inFlight.contains).onComplete {
        case Success(_) => ()
        case Failure(e) =>
          logger.warn(safe"Background-query owner sweep failed on ${Safe(hostId)}: ${Safe(e.getMessage)}")
      }
    }(ec)
  }

  /** Cancel an in-flight execution on this host, if present: the stream is aborted (a failure, so
    * destinations never mistake the truncation for a clean completion) and the record transitions
    * to the terminal [[ExecutionAction.Cancelled]] state, retained until expiry. Returns true iff
    * this host was running it.
    */
  def cancel(executionId: UUID): Boolean =
    inFlight.remove(executionId) match {
      case Some(killSwitch) =>
        killSwitch.abort(BackgroundQueryRunner.ExecutionCancelled)
        true
      case None => false
    }

  /** Run the query, returning a handle to its two observable moments: `started` (the `Started`
    * record write has resolved — a dispatcher that returns the execution id to a client waits on
    * this, so a `GET`/cancel/tap issued the instant it has the id finds the record) and `done` (the
    * execution reached a terminal state and that outcome was handled — failures are recorded, not
    * propagated).
    *
    * @param executionId identity of this execution, minted by the dispatcher
    * @param jobName     the dispatching job, if any, linked into the record
    */
  def run(executionId: UUID, jobName: Option[String], backgroundQuery: BackgroundQuery): Execution = {
    val namespace: NamespaceId = backgroundQuery.namespace.map(NamespaceId(_)).getOrElse(defaultNamespaceId)
    val parameters: Map[String, Value] = backgroundQuery.parameters.view.mapValues(Value.fromJson).toMap

    def record(action: ExecutionAction): BackgroundQueryRecord =
      // Every record carries a finite expiry (now + statusExpiry); the terminal write re-stamps it,
      // so retention counts from termination. On a Started record the stamp is a grace period, not
      // a visibility bound — reads always show Started records and the owner sweep skips in-flight
      // runs — so it only takes effect once the run is dead (host restart, lost terminal write).
      BackgroundQueryRecord(
        executionId = executionId,
        jobName = jobName,
        namespace = namespace.name, // the resolved namespace the query actually ran in
        hostId = hostId,
        name = backgroundQuery.name,
        query = backgroundQuery.query,
        lastAction = action,
        expiresAtMillis = Milliseconds.currentTime().millis + backgroundQuery.statusExpiryMillis,
      )

    // Registered before the Started write and materialization: a SharedKillSwitch remembers an
    // abort that precedes materialization, so a cancel arriving during startup is still honored.
    val killSwitch: SharedKillSwitch = KillSwitches.shared(executionId.toString)
    inFlight.update(executionId, killSwitch)

    // Best-effort wiretap: rows are offered to subscribers under the execution's topic, buffered
    // (capped) until one attaches. Fed on the stream path below; resolved in `finish`.
    val tapRelay =
      new BackgroundQueryTapRelay(
        tapBus,
        TapBus.topicForBackgroundQuery(namespace, executionId),
        graph.system.scheduler,
      )

    val started = Promise[Unit]()
    val done = Promise[Unit]()

    // Sequence: record Started, then run, then record the terminal outcome — so the terminal write
    // is always issued after the Started write, never reordered ahead of it.
    registry.put(record(ExecutionAction.Started())).onComplete { startedWrite =>
      // The run is dispatched either way, so the dispatcher is released even when the write failed
      // (the terminal write below is what will then make the execution visible).
      startedWrite.failed.foreach { e =>
        logger.warn(
          safe"Failed to record the start of background-query execution ${Safe(executionId.toString)}; it will first become visible on completion: ${Safe(e.getMessage)}",
        )
      }
      started.trySuccess(())

      def finish(terminal: ExecutionAction): Unit = {
        inFlight.remove(executionId)
        tapRelay.complete(terminal)
        // The terminal write is gated on the record still existing — the backstop for a record
        // removed out from under a live run (e.g. the manager sweeping a partitioned host's
        // records): the late finish stays silent instead of resurrecting a record the cluster
        // disposed of. The gate only applies where it can mean that: if the Started write itself
        // failed there is nothing to have been swept, and gating would leave the execution with no
        // record at all. A failed gate read writes anyway, for the same reason — better to lose the
        // gate than a live record's outcome.
        val gate: Future[Boolean] =
          if (startedWrite.isFailure) Future.successful(true)
          else registry.get(executionId).map(_.isDefined).recover { case _ => true }
        gate
          .flatMap {
            case true => registry.put(record(terminal))
            case false => Future.unit // retention lapsed mid-run; finish silently
          }
          .onComplete {
            case Success(_) => done.trySuccess(())
            case Failure(e) =>
              // The run itself finished; the scheduler must still be released. Log the lost record.
              logger.warn(
                safe"Failed to record terminal state for background-query execution ${Safe(executionId.toString)}: ${Safe(e.getMessage)}",
              )
              done.trySuccess(())
          }
      }

      // Compile once up front: a compile error is deterministic, so record Failed without retrying.
      Try(queryCypherValues(backgroundQuery.query, namespace, parameters)(graph)) match {
        case Failure(compileError) =>
          finish(ExecutionAction.Failed(compileError.getMessage))
        case Success(running) =>
          val columns: Vector[String] = running.columns.map(_.name)

          // Build the destination sinks, then stream every row (a QueryContext, carrying column
          // names) to all of them, counting rows for the status record.
          Future.sequence(backgroundQuery.destinations.toList.map(destinationSink)).onComplete {
            case Failure(buildError) =>
              finish(ExecutionAction.Failed(buildError.getMessage))
            case Success(folds) =>
              val sinks = folds.zipWithIndex.map { case (fold, idx) =>
                fold.sink[QueryContext](s"background-query/$executionId/$idx", namespace)
              }
              val counted: Future[Long] = running.resultsWithContext
                .via(killSwitch.flow)
                .map { row => tapRelay.offer(row); row } // wiretap leg: non-blocking, never fails
                .alsoToAll(sinks: _*)
                .runWith(Sink.fold(0L)((count, _) => count + 1))(graph.materializer)

              counted.onComplete {
                case Success(count) => finish(ExecutionAction.Completed(count, columns))
                case Failure(BackgroundQueryRunner.ExecutionCancelled) => finish(ExecutionAction.Cancelled())
                case Failure(runError) => finish(ExecutionAction.Failed(runError.getMessage))
              }
          }
      }
    }
    Execution(started.future, done.future)
  }
}

object BackgroundQueryRunner {

  /** Handle on a dispatched execution: `started` completes once the `Started` record write has
    * resolved, `done` once the execution has reached a terminal state and that outcome has been
    * handled. Neither ever fails — an execution's failures live in its record, not in these.
    */
  final case class Execution(started: Future[Unit], done: Future[Unit])

  /** Sentinel failure [[BackgroundQueryRunner.cancel]] aborts an execution's stream with; matched by
    * reference in the terminal handler to record [[ExecutionAction.Cancelled]] rather than Failed.
    * A shared stackless singleton — its stack trace carries no information.
    */
  object ExecutionCancelled extends RuntimeException("Background query execution cancelled") with NoStackTrace

  /** Owner-sweep cadence. Lazy on purpose: expiry grants days of grace, promptness buys nothing. */
  val OwnerSweepInterval: FiniteDuration = 10.minutes
}

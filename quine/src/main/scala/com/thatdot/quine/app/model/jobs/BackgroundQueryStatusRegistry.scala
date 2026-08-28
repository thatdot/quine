package com.thatdot.quine.app.model.jobs

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

import io.circe.parser
import io.circe.syntax._

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.graph.scheduledjob.ScheduledJobSweeper
import com.thatdot.quine.model.Milliseconds

/** Cluster-visible registry of background-query execution *status* records, one per execution id —
  * result rows stream to destinations and are never stored here. Each record lives under its own
  * metadata key: the executing host (`hostId`) is its sole writer, and any host can read it.
  *
  * Expiry responsibility is split by ownership, because only the owning host knows whether a
  * `Started` record is a live run (its in-memory in-flight set):
  *   - '''Reads''' ([[get]]/[[list]]): terminal records hide once expired; `Started` records are
  *     always visible — their true state is "running or unresolved", resolved by the sweeps below.
  *   - '''Owner sweep''' ([[sweepOwnedBy]], a periodic per-host tick in the runner): deletes this
  *     host's expired records that are not in flight here — a live run's record is never eligible.
  *   - '''Startup reconciliation''' ([[reconcileOwnedStarted]], once as the runner starts): finalizes
  *     this host's own leftover `Started` records to [[ExecutionAction.Interrupted]] — their runs
  *     died with a previous incarnation (their in-memory kill switches did not survive), so they must
  *     not linger as phantom "running" rows until expiry. Clears the single-host phantom; enterprise
  *     hosts get a fresh identity per incarnation, so their pre-restart records fall to the manager.
  *   - '''Manager sweep''' ([[sweepExpired]], the driver's [[ScheduledJobSweeper]] hook), for records
  *     whose owner is absent from the cluster (`hostPresent`): deletes the expired ones, and finalizes
  *     a still-in-retention `Started` record to `Interrupted` regardless of expiry (an absent owner
  *     cannot be running it). Present owners' records are left to the two owner-side paths above. It
  *     also deletes any blob under the prefix that fails to decode: an unreadable record has no
  *     hostId or expiry, so no record-driven sweep could ever see it — this is its only GC.
  *
  * A partitioned-but-alive owner can have an in-flight record manager-finalized; the runner's
  * terminal-write gate lets its eventual finish overwrite the `Interrupted` state with the real
  * outcome rather than resurrect a deleted record.
  */
class BackgroundQueryStatusRegistry(graph: BaseGraph, hostPresent: String => Boolean)(implicit val logConfig: LogConfig)
    extends ScheduledJobSweeper
    with LazySafeLogging {

  implicit private val ec: ExecutionContext = graph.system.dispatcher

  private def key(executionId: UUID): String = BackgroundQueryStatusRegistry.KeyPrefix + executionId.toString

  /** Write (or overwrite) an execution's record. Called by the executing host on each transition. */
  def put(record: BackgroundQueryRecord): Future[Unit] =
    graph.namespacePersistor.setMetaData(key(record.executionId), Some(record.asJson.noSpaces.getBytes(UTF_8)))

  /** Delete one execution's record outright, whatever its state, by execution id. Callable from any
    * host (the record is single-persistor-backed and cluster-visible). Backs the delete endpoint; a
    * still-running execution should be cancelled first. The runner's terminal-write gate keeps that
    * execution's late finish from resurrecting the record — and a record that does slip back is
    * terminal, so it expires on its own schedule rather than lingering as a phantom "running" row.
    */
  def delete(executionId: UUID): Future[Unit] =
    graph.namespacePersistor.setMetaData(key(executionId), None)

  /** Whether a record is visible to reads: terminal records are hidden once expired; `Started`
    * records are always visible (see the sweep rules in the class doc).
    */
  private def visible(record: BackgroundQueryRecord, nowMillis: Long): Boolean = record.lastAction match {
    case ExecutionAction.Started() => true
    case _ => record.expiresAtMillis > nowMillis
  }

  /** Read one execution's record, if present and visible. Callable from any host. */
  def get(executionId: UUID): Future[Option[BackgroundQueryRecord]] =
    graph.namespacePersistor
      .getMetaData(key(executionId))
      .map(_.flatMap(decode).filter(visible(_, Milliseconds.currentTime().millis)))

  /** All visible records, optionally restricted to executions dispatched by one job (by name).
    * Backed by a short-lived cache of the underlying full scan ([[cachedAllRecords]]) so that
    * frequent polling of this endpoint does not scan the whole metadata store on every call.
    * Visibility (which depends on `now`) is still evaluated fresh against the cached record set.
    */
  def list(jobName: Option[String] = None): Future[Vector[BackgroundQueryRecord]] =
    cachedAllRecords().map { records =>
      val now = Milliseconds.currentTime().millis
      records.filter(r => visible(r, now) && jobName.forall(r.jobName.contains))
    }

  /** Owner sweep: delete `hostId`'s expired records that are not in flight there. `inFlight` is the
    * owning runner's live-execution check, so only the owning host may call this.
    */
  def sweepOwnedBy(hostId: String, inFlight: UUID => Boolean): Future[Unit] = {
    val now = Milliseconds.currentTime().millis
    allRecords().flatMap { records =>
      val eligible =
        records.filter(r => r.hostId == hostId && r.expiresAtMillis <= now && !inFlight(r.executionId))
      Future
        .traverse(eligible)(r => graph.namespacePersistor.setMetaData(key(r.executionId), None))
        .map(_ => ())
    }
  }

  /** Startup reconciliation: finalize this host's own leftover `Started` records to
    * [[ExecutionAction.Interrupted]]. Meant to run once, before any execution starts, so `inFlight`
    * is empty and every own `Started` record is by definition a dead run from a previous incarnation
    * (its in-memory kill switch did not survive the restart). It cannot race a finishing run because
    * there are none yet — which is why this, rather than the periodic owner sweep, is where an own
    * `Started` record is finalized without waiting for expiry.
    *
    * OSS host ids are stable (`"local"`), so this clears the single-host phantom. Enterprise hosts
    * take a fresh identity per incarnation, so this matches none of their pre-restart records —
    * those are the manager sweep's responsibility ([[sweepExpired]]).
    */
  def reconcileOwnedStarted(hostId: String, inFlight: UUID => Boolean): Future[Unit] =
    allRecords().flatMap { records =>
      val phantoms =
        records.filter(r => r.hostId == hostId && isStarted(r.lastAction) && !inFlight(r.executionId))
      Future
        .traverse(phantoms)(r => put(r.copy(lastAction = ExecutionAction.Interrupted())))
        .map(_ => ())
    }

  /** Manager sweep (the [[ScheduledJobSweeper]] contract), for records whose owning host is absent
    * from the cluster (present owners are left to their own owner sweep / startup reconciliation):
    *   - expired records are deleted (past retention, any action);
    *   - a still-in-retention `Started` record is finalized to [[ExecutionAction.Interrupted]]
    *     regardless of expiry — an absent owner cannot be running it, so it is a dead run, and unlike
    *     the owner it cannot be mid-finish. A partitioned-but-alive owner that later finishes
    *     overwrites this with its real outcome (its terminal-write gate still sees the record).
    */
  override def sweepExpired(now: Milliseconds): Future[Unit] =
    // Raw scan (not allRecords) so undecodable blobs are visible here as keys — allRecords drops
    // them, so they would otherwise be immortal (no owner/expiry to drive any other sweep).
    graph.namespacePersistor.getAllMetaData().flatMap { all =>
      val (records, undecodableKeys) = {
        val decoded = Vector.newBuilder[BackgroundQueryRecord]
        val undecodable = Vector.newBuilder[String]
        all.iterator.foreach {
          case (k, bytes) if k.startsWith(BackgroundQueryStatusRegistry.KeyPrefix) =>
            decode(bytes) match {
              case Some(r) => decoded += r
              case None => undecodable += k
            }
          case _ => ()
        }
        (decoded.result(), undecodable.result())
      }
      val absentOwner = records.filter(r => !hostPresent(r.hostId))
      val (expired, inRetention) = absentOwner.partition(_.expiresAtMillis <= now.millis)
      val toInterrupt = inRetention.filter(r => isStarted(r.lastAction))
      if (undecodableKeys.nonEmpty)
        logger.warn(
          safe"Deleting ${Safe(undecodableKeys.size.toString)} undecodable background-query status record(s) during sweep",
        )
      for {
        _ <- Future.traverse(expired)(r => graph.namespacePersistor.setMetaData(key(r.executionId), None))
        _ <- Future.traverse(toInterrupt)(r => put(r.copy(lastAction = ExecutionAction.Interrupted())))
        _ <- Future.traverse(undecodableKeys)(k => graph.namespacePersistor.setMetaData(k, None))
      } yield ()
    }

  private def isStarted(action: ExecutionAction): Boolean = action match {
    case ExecutionAction.Started() => true
    case _ => false
  }

  private def allRecords(): Future[Vector[BackgroundQueryRecord]] =
    graph.namespacePersistor.getAllMetaData().map { all =>
      all.iterator
        .collect {
          case (k, bytes) if k.startsWith(BackgroundQueryStatusRegistry.KeyPrefix) => decode(bytes)
        }
        .flatten
        .toVector
    }

  // Short-TTL cache of the full scan, guarding only the read path ([[list]]). `getAllMetaData` scans
  // the entire metadata store (Cassandra `SELECT *` / RocksDB full column-family read), so uncached
  // polling of the list endpoint could load the persistor's blocking IO. The cache is global (one
  // shared scan) and single-flight (concurrent misses share one in-flight scan), so however many
  // clients poll however fast, at most one scan runs per [[ListCacheTtl]]. Deliberately not used by
  // the sweeps or reconciliation — those must act on fresh state to delete/finalize correctly.
  private val cacheLock = new Object
  private var cacheExpiryMillis: Long = Long.MinValue
  private var cachedScan: Future[Vector[BackgroundQueryRecord]] = Future.successful(Vector.empty)

  private def cachedAllRecords(): Future[Vector[BackgroundQueryRecord]] = cacheLock.synchronized {
    val now = Milliseconds.currentTime().millis
    if (now < cacheExpiryMillis) cachedScan
    else {
      val fresh = allRecords()
      cachedScan = fresh
      cacheExpiryMillis = now + BackgroundQueryStatusRegistry.ListCacheTtl.toMillis
      // Never serve a cached failure: if the scan fails, expire immediately so the next call rescans.
      fresh.onComplete {
        case Failure(_) => cacheLock.synchronized(if (cachedScan eq fresh) cacheExpiryMillis = Long.MinValue)
        case _ => ()
      }
      fresh
    }
  }

  private def decode(bytes: Array[Byte]): Option[BackgroundQueryRecord] =
    parser.decode[BackgroundQueryRecord](new String(bytes, UTF_8)).toOption
}

object BackgroundQueryStatusRegistry {

  /** Metadata key prefix; each record is stored under `background_query/<executionId>`. */
  val KeyPrefix: String = "background_query/"

  /** How long a full-scan result is reused to serve the list endpoint before a rescan. Short enough
    * that polled status stays fresh (results stream separately; status is best-effort anyway), long
    * enough that a poll storm collapses to a trickle of scans on the persistor's blocking IO.
    */
  val ListCacheTtl: FiniteDuration = 1.second
}

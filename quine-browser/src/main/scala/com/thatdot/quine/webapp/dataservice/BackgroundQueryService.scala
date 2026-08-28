package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.{Observer, Signal}
import io.circe.Json

import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2BackgroundQueryStatus

/** Background-query capability: out-of-band Cypher runs in the current graph — starting them,
  * cancelling them, and the list of their execution records.
  *
  * Only *status* lives here. A run's result rows are streamed to its configured destinations
  * and, for the UI, to its `:tap` WebSocket — see [[WiretapService.OpenBackgroundQueryTap]],
  * which is the capability that watches a run rather than merely tracking it.
  *
  * Executions are scoped to the current graph namespace, like standing queries and ingests;
  * switching graphs re-scopes the list. (The *jobs* that dispatch them are cluster-wide — see
  * [[JobService]].)
  */
trait BackgroundQueryService {

  /** Entry point for background-query commands; see [[NamespaceService.namespaceDispatch]]
    * for why each slice has its own dispatch.
    */
  def backgroundQueryDispatch: Observer[BackgroundQueryService.Command]

  /** Unexpired execution records for the current graph namespace, newest activity first.
    * `Pot` distinguishes loading and fetch failure (with `FailedStale` keeping the last good
    * list) from a truly empty list.
    */
  def backgroundQueriesSignal: Signal[Pot[Seq[V2BackgroundQueryStatus]]]
}

object BackgroundQueryService {

  /** A state-changing request to the background-query capability, sent via
    * [[BackgroundQueryService.backgroundQueryDispatch]].
    */
  sealed trait Command

  /** Request an immediate refetch of [[BackgroundQueryService.backgroundQueriesSignal]]
    * instead of waiting out the poll interval.
    */
  case object RefreshBackgroundQueries extends Command

  /** Start a background query in the current graph. `body` is the assembled
    * `BackgroundQueryDef` — the caller builds it, because the destination shape is a large
    * open union this slice has no reason to model.
    */
  final case class RunBackgroundQuery(
    body: Json,
    replyTo: Observer[RunResult] = Observer.empty,
  ) extends Command

  /** Ask the cluster to cancel an execution. Cancelling an already-terminal execution is a
    * no-op, and the transition to `cancelled` lands on a later poll rather than immediately.
    */
  final case class CancelBackgroundQuery(
    id: String,
    replyTo: Observer[SaveResult] = Observer.empty,
  ) extends Command

  /** Delete an execution's status record. A still-running execution is cancelled first
    * (cluster-wide), then its record is dropped immediately rather than waiting out its expiry.
    * Deleting an already-absent execution is a no-op from the UI's point of view.
    */
  final case class DeleteBackgroundQuery(
    id: String,
    replyTo: Observer[SaveResult] = Observer.empty,
  ) extends Command

  /** The outcome of a [[RunBackgroundQuery]]. Distinct from the shared [[SaveResult]] because
    * success carries the new execution id — the id the caller immediately opens a tap on.
    */
  sealed trait RunResult
  final case class RunStarted(executionId: String) extends RunResult
  final case class RunFailed(message: String) extends RunResult
}

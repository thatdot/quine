package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.{Observer, Signal}

import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2JobStatus

/** Scheduled-job capability: jobs that dispatch an action (today, a background query) on a
  * recurring schedule.
  *
  * Unlike every other list slice here, jobs are **cluster-wide, not per-graph**: a job's target
  * graph lives in its action, not in its URL, so [[jobsSignal]] does not re-scope when the user
  * switches graphs. The API does not report the action either, which is why the UI can list,
  * create, and delete jobs but not edit one.
  */
trait JobService {

  /** Entry point for scheduled-job commands; see [[NamespaceService.namespaceDispatch]] for
    * why each slice has its own dispatch.
    */
  def jobDispatch: Observer[JobService.Command]

  /** Every scheduled job in the cluster. `Pot` distinguishes loading and fetch failure (with
    * `FailedStale` keeping the last good list) from a truly empty list.
    */
  def jobsSignal: Signal[Pot[Seq[V2JobStatus]]]
}

object JobService {

  /** A state-changing request to the scheduled-job capability, sent via
    * [[JobService.jobDispatch]].
    */
  sealed trait Command

  /** Request an immediate refetch of [[JobService.jobsSignal]], e.g. after a create or delete,
    * instead of waiting out the poll interval.
    */
  case object RefreshJobs extends Command
}

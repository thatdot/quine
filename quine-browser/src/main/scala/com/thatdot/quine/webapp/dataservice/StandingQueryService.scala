package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.{Observer, Signal}

import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2StandingQueryInfo

/** Standing-query capability: the current graph's standing-query list. */
trait StandingQueryService {

  /** Entry point for standing-query commands; see [[NamespaceService.namespaceDispatch]]
    * for why each slice has its own dispatch.
    */
  def standingQueryDispatch: Observer[StandingQueryService.Command]

  /** Standing queries for the current graph namespace; `Pot` distinguishes loading and
    * fetch failure (with `FailedStale` keeping the last good list) from a truly empty list.
    */
  def standingQueriesSignal: Signal[Pot[Seq[V2StandingQueryInfo]]]
}

object StandingQueryService {

  /** A state-changing request to the standing-query capability, sent via
    * [[StandingQueryService.standingQueryDispatch]].
    */
  sealed trait Command

  /** Request an immediate refetch of [[StandingQueryService.standingQueriesSignal]], e.g.
    * after a mutation, instead of waiting out the poll interval.
    */
  case object RefreshStandingQueries extends Command
}

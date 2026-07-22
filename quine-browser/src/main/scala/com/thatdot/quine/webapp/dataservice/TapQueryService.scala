package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.{Observer, Signal}

import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2TapQuery

/** Saved tap-query capability: the current graph's persisted tap-query list. Live tap
  * connections are a separate capability ([[WiretapService]]) — this slice is the CRUD
  * surface for the definitions those taps are opened from.
  */
trait TapQueryService {

  /** Entry point for tap-query commands; see [[NamespaceService.namespaceDispatch]] for
    * why each slice has its own dispatch.
    */
  def tapQueryDispatch: Observer[TapQueryService.Command]

  /** Saved tap queries for the current graph namespace. A [[Pot]] so consumers can tell
    * "not loaded yet" from "loaded, empty" — [[TapQueryService.SaveTapQueries]] replaces
    * the whole list, so mutations must not be built from a list that never loaded.
    */
  def tapQueriesSignal: Signal[Pot[Vector[V2TapQuery]]]
}

object TapQueryService {

  /** A state-changing request to the tap-query capability, sent via
    * [[TapQueryService.tapQueryDispatch]].
    */
  sealed trait Command

  /** Replace the CURRENT graph's tap-query list; on success
    * [[TapQueryService.tapQueriesSignal]] refetches.
    */
  final case class SaveTapQueries(
    tapQueries: Vector[V2TapQuery],
    replyTo: Observer[SaveResult] = Observer.empty,
  ) extends Command
}

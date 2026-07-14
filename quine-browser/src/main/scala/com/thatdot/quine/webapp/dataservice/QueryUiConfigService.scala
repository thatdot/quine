package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.{Observer, Signal}

import com.thatdot.quine.routes.{SampleQuery, UiNodeAppearance}
import com.thatdot.quine.v2api.routes.V2UiNodeQuickQuery

/** Query-UI configuration capability: the user-editable settings that shape the explorer
  * (saved sample queries, node quick queries, node appearance rules). All three are
  * global — not graph-scoped — and each save is a whole-list replace.
  */
trait QueryUiConfigService {

  /** Entry point for query-UI configuration commands; see
    * [[NamespaceService.namespaceDispatch]] for why each slice has its own dispatch.
    */
  def queryUiConfigDispatch: Observer[QueryUiConfigService.Command]

  /** Saved sample queries shown in the query bar dropdown and bookmark dialog. */
  def sampleQueriesSignal: Signal[Vector[SampleQuery]]

  /** Quick queries offered in the node context menu, paired with their node predicates. */
  def quickQueriesSignal: Signal[Vector[V2UiNodeQuickQuery]]

  /** Node appearance rules (icon, color, size, label) applied when rendering graph nodes. */
  def nodeAppearancesSignal: Signal[Vector[UiNodeAppearance]]
}

object QueryUiConfigService {

  /** A state-changing request to the query-UI configuration capability, sent via
    * [[QueryUiConfigService.queryUiConfigDispatch]].
    */
  sealed trait Command

  /** Persist the full sample-query list (whole-list replace). On success
    * [[QueryUiConfigService.sampleQueriesSignal]] refetches immediately and `replyTo`
    * receives [[SaveSucceeded]]; on failure `replyTo` receives [[SaveFailed]].
    */
  final case class SaveSampleQueries(
    sampleQueries: Vector[SampleQuery],
    replyTo: Observer[SaveResult] = Observer.empty,
  ) extends Command

  /** Persist the full quick-query list; on success
    * [[QueryUiConfigService.quickQueriesSignal]] refetches.
    */
  final case class SaveQuickQueries(
    quickQueries: Vector[V2UiNodeQuickQuery],
    replyTo: Observer[SaveResult] = Observer.empty,
  ) extends Command

  /** Persist the full node-appearance list; on success
    * [[QueryUiConfigService.nodeAppearancesSignal]] refetches (picking up any server-side
    * canonicalization).
    */
  final case class SaveNodeAppearances(
    appearances: Vector[UiNodeAppearance],
    replyTo: Observer[SaveResult] = Observer.empty,
  ) extends Command
}

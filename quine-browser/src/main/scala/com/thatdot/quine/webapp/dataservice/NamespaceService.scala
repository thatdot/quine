package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.{Observer, Signal}

import com.thatdot.quine.routes.exts.NamespaceParameter

/** Graph-namespace capability: which graphs exist and which one is selected. The selected
  * namespace is the keying spine of the data layer — every graph-scoped signal on the
  * sibling slices (standing queries, tap queries, ingest streams, wiretaps) follows it.
  */
trait NamespaceService {

  /** Entry point for namespace commands. Each capability slice seals its own command
    * vocabulary behind its own dispatch, so a component declaring only this slice cannot
    * send another slice's commands. Implementations ignore commands that do not apply to
    * them.
    */
  def namespaceDispatch: Observer[NamespaceService.Command]

  /** Known graph namespaces; a single-element list on OSS, where only `quine` exists. */
  def namespacesSignal: Signal[Seq[NamespaceParameter]]

  /** The selected graph namespace, keying every graph-scoped signal. Falls back to a
    * known namespace when the requested one is absent (e.g. deleted from another session).
    */
  def currentNamespaceSignal: Signal[NamespaceParameter]
}

object NamespaceService {

  /** A state-changing request to the namespace capability, sent via
    * [[NamespaceService.namespaceDispatch]].
    */
  sealed trait Command

  /** Request selecting `namespace` as the current graph. Takes effect only once the
    * namespace is (or becomes) known; see [[NamespaceService.currentNamespaceSignal]].
    */
  final case class SetNamespace(namespace: NamespaceParameter) extends Command

  /** Request an immediate refetch of [[NamespaceService.namespacesSignal]], e.g. after
    * creating or deleting a graph, instead of waiting out the poll interval.
    */
  case object RefreshNamespaces extends Command
}

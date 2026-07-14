package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.{Observer, Signal}

import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2TapQuery

/** Live-tap capability: standing-query match streams, exposed as signals and driven by
  * commands. That the implementation feeds them over WebSockets is as hidden as any other
  * transport choice.
  *
  * Taps are scoped to the current graph namespace — switching graphs closes every open tap.
  * Locally-enabled tap queries are remembered per graph for this browser tab and restored
  * when the graph is revisited or the page reloads.
  *
  * Deliberately NOT here: acting on a match (the explorer's dispatch host runs the tap
  * query with synthetic edges when one arrives) — that drives the query engine and graph
  * rendering, and stays a consumer concern forever.
  */
trait WiretapService {

  /** Entry point for wiretap commands. A separate channel from [[DataService.dispatch]]:
    * the two command vocabularies are sealed independently, and a component declaring only
    * [[WiretapService]] gets a dispatch that cannot carry namespace or refresh commands.
    */
  def wiretapDispatch: Observer[WiretapService.Command]

  /** Open tap handlers, grouped by the UI surface that asked for them. Each handler
    * bundles its own status / match-count / message signals; two consumers using the same
    * `(owner, key)` share one handler, and handlers on the same source share the
    * underlying connection.
    */
  def wiretapsSignal: Signal[Map[WiretapOwner, List[WiretapHandler]]]

  /** Tap queries the user has enabled locally in this browser session, keyed by tap-query
    * name. Joined with [[wiretapsSignal]] by the explorer's match-dispatch host: only
    * names present in both are dispatched.
    */
  def enabledTapQueriesSignal: Signal[Map[String, V2TapQuery]]
}

object WiretapService {

  /** Owner for taps the service opens from "Enable locally" tap-query intent. The
    * per-handler `key` within this owner is the tap query's name, so multiple tap queries
    * targeting the same `(sqName, outputName)` each get their own handler (sharing the
    * underlying connection).
    */
  val TapQueryOwner: WiretapOwner = WiretapOwner("tapQuery")

  /** A state-changing request to the wiretap capability, sent via
    * [[WiretapService.wiretapDispatch]].
    */
  sealed trait Command

  /** Open a tap for `(owner, key)` on the given standing-query source; no-op if that pair
    * is already open.
    *
    * @param tapPoint which point in the standing query's output pipeline to tap - raw,
    *   pre-enrichment on an output, or post-enrichment on an output
    */
  final case class OpenTap(owner: WiretapOwner, key: String, sqName: String, tapPoint: WiretapTapPoint) extends Command

  /** Close the tap for `(owner, key)`; no-op if none is open. */
  final case class CloseTap(owner: WiretapOwner, key: String) extends Command

  /** Enable a tap query locally: the service opens its tap under [[TapQueryOwner]],
    * remembers the intent per graph for this browser tab, and keeps the tap current
    * across reloads and server-side edits (see [[WiretapService.enabledTapQueriesSignal]]).
    */
  final case class EnableTapQuery(tapQuery: V2TapQuery) extends Command

  /** Disable a locally-enabled tap query: closes its tap and forgets the intent. */
  final case class DisableTapQuery(name: String) extends Command
}

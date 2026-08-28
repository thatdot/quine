package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.{Observer, Signal}

import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2GraphFeed

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
  def enabledGraphFeedsSignal: Signal[Map[String, V2GraphFeed]]

  /** Open background-query result taps, keyed by execution id.
    *
    * Unlike [[wiretapsSignal]] these are not grouped by owner. Several surfaces can watch one
    * execution at once — an Explorer result card and the Streams page's inspection typically do
    * — and rather than scoping the map per owner the store refcounts subscribers behind it, so
    * the socket survives until the last of them closes (see [[OpenBackgroundQueryTap]]).
    *
    * The consequence for a consumer is that this map shows every open background-query tap, not
    * just the ones that consumer opened.
    */
  def backgroundQueryTapsSignal: Signal[Map[String, BackgroundQueryTapHandler]]
}

object WiretapService {

  /** Owner for taps the service opens from "Show on my graph" tap-query intent. The
    * per-handler `key` within this owner is the tap query's name, so multiple tap queries
    * targeting the same `(sqName, outputName)` each get their own handler (sharing the
    * underlying connection).
    */
  val GraphFeedOwner: WiretapOwner = WiretapOwner("graphFeed")

  /** A state-changing request to the wiretap capability, sent via
    * [[WiretapService.wiretapDispatch]].
    */
  sealed trait Command

  /** Open a tap for `(owner, key)` on the given standing-query source; no-op if that pair
    * is already open.
    *
    * @param tapPoint which point in the standing query's output pipeline to tap - raw,
    *   or post-enrichment on an output
    */
  final case class OpenTap(owner: WiretapOwner, key: String, sqName: String, tapPoint: WiretapTapPoint) extends Command

  /** Close the tap for `(owner, key)`; no-op if none is open. */
  final case class CloseTap(owner: WiretapOwner, key: String) extends Command

  /** Enable a tap query locally: the service opens its tap under [[GraphFeedOwner]],
    * remembers the intent per graph for this browser tab, and keeps the tap current
    * across reloads and server-side edits (see [[WiretapService.enabledGraphFeedsSignal]]).
    */
  final case class EnableGraphFeed(graphFeed: V2GraphFeed) extends Command

  /** Disable a locally-enabled graph feed: closes its tap and forgets the intent. */
  final case class DisableGraphFeed(name: String) extends Command

  /** Open a tap on a background-query execution's result stream; no-op if `subscriber` already
    * has one.
    *
    * @param subscriber the surface watching — an Explorer result card and the Streams page's
    *                   viewer can watch the same run at once, and the underlying socket is
    *                   released only when the last of them closes
    * @param displayName what to label the stream with — the run's name, else its query text
    */
  final case class OpenBackgroundQueryTap(subscriber: String, executionId: String, displayName: String) extends Command

  /** Release `subscriber`'s background-query tap; no-op if it holds none. Note this only stops
    * *watching*: the run itself keeps going (cancel it via
    * [[BackgroundQueryService.CancelBackgroundQuery]]), and once it has finished the tap cannot
    * be reopened.
    */
  final case class CloseBackgroundQueryTap(subscriber: String, executionId: String) extends Command

  /** Subscriber name for the Streams page's results viewer. The Explorer's cards use their
    * [[WiretapOwner]]'s name, so the two never collide.
    */
  val StreamsPageSubscriber: String = "streamsPage"
}

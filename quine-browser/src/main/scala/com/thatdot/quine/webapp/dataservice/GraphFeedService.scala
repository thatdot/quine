package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.{Observer, Signal}

import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2GraphFeed

/** Saved tap-query capability: the current graph's persisted tap-query list. Live tap
  * connections are a separate capability ([[WiretapService]]) — this slice is the CRUD
  * surface for the definitions those taps are opened from.
  */
trait GraphFeedService {

  /** Entry point for tap-query commands; see [[NamespaceService.namespaceDispatch]] for
    * why each slice has its own dispatch.
    */
  def graphFeedDispatch: Observer[GraphFeedService.Command]

  /** Saved tap queries for the current graph namespace. A [[Pot]] so consumers can tell
    * "not loaded yet" from "loaded, empty" — [[GraphFeedService.SaveGraphFeeds]] replaces
    * the whole list, so mutations must not be built from a list that never loaded.
    */
  def graphFeedsSignal: Signal[Pot[Vector[V2GraphFeed]]]
}

object GraphFeedService {

  /** A state-changing request to the tap-query capability, sent via
    * [[GraphFeedService.graphFeedDispatch]].
    */
  sealed trait Command

  /** Replace the CURRENT graph's tap-query list; on success
    * [[GraphFeedService.graphFeedsSignal]] refetches.
    */
  final case class SaveGraphFeeds(
    graphFeeds: Vector[V2GraphFeed],
    replyTo: Observer[SaveResult] = Observer.empty,
  ) extends Command
}

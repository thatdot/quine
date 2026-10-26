package com.thatdot.quine.app.model.outputs2.destination

import com.thatdot.quine.app.model.outputs2.QuineResultDestination

/** What a user configures on a Novelty standing-query output destination, independent of the API
  * type that carries it and of the implementation that runs it.
  */
final case class NoveltyStreamSettings(
  ownerId: String,
  endpoints: List[String],
  model: String,
  scoreEdgeLabel: String,
  parallelism: Int,
  shouldRetry: Boolean,
  auth: Option[NoveltyAuth],
)

/** Capability supplying the Novelty standing-query output destination.
  *
  * The Novelty destination's API type is compiled into OSS — the destinations ADT
  * (`QuineDestinationSteps`) is `sealed` in the public module, so the variant cannot live only in
  * Enterprise — but its implementation is not: the destination itself exists only in Quine
  * Enterprise, which hands it to the API→outputs2 converter through [[Available]]. OSS supplies
  * [[Unavailable]], so the OSS runtime has nothing to construct even on a code path that skips the
  * `QuineApiMethods.validateWorkflow` check. It complements that validation gate (which still
  * produces the clean user-facing error) as defense in depth.
  *
  * There is deliberately no default: every caller of the converters states which it supplies, so
  * an Enterprise call site cannot silently fall back to [[Unavailable]].
  */
sealed trait NoveltyDestinationSupport
object NoveltyDestinationSupport {

  /** @param build constructs the destination for the given settings */
  final case class Available(build: NoveltyStreamSettings => QuineResultDestination.FoldableData.NoveltyStream)
      extends NoveltyDestinationSupport
  case object Unavailable extends NoveltyDestinationSupport

  /** Why a Novelty destination could not be built under [[Unavailable]]. */
  final class UnavailableException
      extends UnsupportedOperationException("The Novelty output destination is only available in Quine Enterprise.")
}

package com.thatdot.quine.webapp.resultspanel

/** Which point in a standing query's pipeline a tap observes, in pipeline order:
  *   - [[TapPoint.Raw]] — raw StandingQueryResults, before any output workflow (per SQ).
  *   - [[TapPoint.PreEnrichment]] — after the output's `preEnrichmentTransformation`, before its
  *     Cypher enrichment (per output). Only meaningful when the output defines a transformation;
  *     without one the stream is identical to Raw, so the UI never offers it there.
  *   - [[TapPoint.PostEnrichment]] — after the output's Cypher enrichment, what the destination
  *     receives (per output). Only meaningful when the output defines an enrichment query;
  *     without one the stream is identical to the previous stage, so the UI never offers Post
  *     there.
  */
sealed abstract class TapPoint
object TapPoint {
  case object Raw extends TapPoint
  final case class PreEnrichment(output: String) extends TapPoint
  final case class PostEnrichment(output: String) extends TapPoint
}

/** Identifies a tap source — a live server-side stream the panel can subscribe to. This is the
  * one currency for "which tap" — commands, the capability, and the wiretap stores all speak it —
  * so the panel never parses a producer-owned [[LiveSource.id]] whose format it does not control.
  *
  * Consumers that hold a target treat it opaquely: they compare it, and read [[key]] and
  * [[label]]. That is what lets the whole card machinery
  * ([[com.thatdot.quine.webapp.resultspanel.cards.CardsStore]]) serve both variants without
  * branching on which one it has.
  */
sealed abstract class TapTarget {

  /** The canonical stable, unique string for this target — the consumer key passed to
    * [[TapSubscriptions.open]] and the wiretap stores' per-source key alike. The `:`
    * separators are collision-proof (resource names, per AIP-122, forbid colons) and echo the
    * V2 API's own `:tap` verb delimiter.
    */
  def key: String

  /** Human-readable provenance label — the card title and the source chip. */
  def label: String

  /** Whether a tap on this target can be reopened after it has ended.
    *
    * The dividing line is whether the source outlives one subscription. A standing query keeps
    * producing, so a fresh socket resumes its stream — which is what the card layer's whole
    * freeze/reopen continuation protocol is built on. A background query is one finite run whose
    * server-side relay is torn down when it terminates, so nothing can be resumed and every
    * "pause now, get more later" affordance is a dead end.
    *
    * Consumers use this to decide three things: whether to open the tap under a sample budget,
    * whether pausing it is safe, and whether to offer the paused-state exits at all.
    */
  def resumable: Boolean
}

object TapTarget {

  /** A standing query plus the pipeline stage being tapped. Open-ended: the standing query
    * keeps producing, so a tap that ends can always be reopened to resume the stream.
    */
  final case class StandingQuery(sqName: String, tapPoint: TapPoint) extends TapTarget {

    val key: String = tapPoint match {
      case TapPoint.Raw => s"$sqName:raw"
      case TapPoint.PreEnrichment(out) => s"$sqName:pre:$out"
      case TapPoint.PostEnrichment(out) => s"$sqName:post:$out"
    }

    /** e.g. `fraud · Standing Query Results` or `fraud/slack · enriched`. */
    val label: String = tapPoint match {
      case TapPoint.Raw => s"$sqName · Standing Query Results"
      case TapPoint.PreEnrichment(out) => s"$sqName/$out · transformed"
      case TapPoint.PostEnrichment(out) => s"$sqName/$out · enriched"
    }

    val resumable: Boolean = true
  }

  /** One background-query execution's result stream. `executionId` is the server-minted id
    * from `POST .../backgroundQueries`; `displayName` is the run's name, else its truncated
    * query text.
    *
    * Unlike a [[StandingQuery]] tap this is finite and single-shot — see [[resumable]].
    */
  final case class BackgroundQuery(executionId: String, displayName: String) extends TapTarget {

    // `bq:` can never collide with a StandingQuery key: standing query names cannot contain
    // a colon, so no SQ key can begin with this prefix.
    val key: String = s"$BackgroundQueryKeyPrefix$executionId"

    val label: String = displayName

    val resumable: Boolean = false
  }

  val BackgroundQueryKeyPrefix: String = "bq:"
}

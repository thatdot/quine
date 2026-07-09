package com.thatdot.quine.webapp.resultspanel

/** Which point in a standing query's pipeline a tap observes. Three exist server-side, in
  * pipeline order:
  *   - [[TapPoint.Raw]] — raw StandingQueryResults, before any output workflow (per SQ).
  *   - [[TapPoint.PreEnrichment]] — after the output's `preEnrichmentTransformation`, before
  *     its Cypher enrichment (per output).
  *   - [[TapPoint.PostEnrichment]] — after all stages, what the destination receives (per output).
  */
sealed abstract class TapPoint
object TapPoint {
  case object Raw extends TapPoint
  final case class PreEnrichment(output: String) extends TapPoint
  final case class PostEnrichment(output: String) extends TapPoint
}

/** Identifies a tap source: a standing query plus the pipeline stage being tapped. This is the
  * one currency for "which tap" — commands, the capability, and the wiretap store all speak it —
  * so the panel never parses a producer-owned [[LiveSource.id]] whose format it does not control.
  */
final case class TapTarget(sqName: String, tapPoint: TapPoint) {

  /** The canonical stable, unique string for this target — the consumer key passed to
    * [[TapSubscriptions.open]]/`close` and the wiretap store's per-source key alike. The `:`
    * separators are collision-proof (resource names, per AIP-122, forbid colons) and echo the
    * V2 API's own `:tap` verb delimiter.
    */
  val key: String = tapPoint match {
    case TapPoint.Raw => s"$sqName:raw"
    case TapPoint.PreEnrichment(out) => s"$sqName:pre:$out"
    case TapPoint.PostEnrichment(out) => s"$sqName:post:$out"
  }

  /** Human-readable provenance label, e.g. `fraud · raw` or `fraud/slack · post`. */
  val label: String = tapPoint match {
    case TapPoint.Raw => s"$sqName · raw"
    case TapPoint.PreEnrichment(out) => s"$sqName/$out · pre"
    case TapPoint.PostEnrichment(out) => s"$sqName/$out · post"
  }
}

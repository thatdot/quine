package com.thatdot.quine.webapp.resultspanel

/** One output of a standing query, as the tap picker sees it: its name and which optional
  * workflow steps it defines. Step presence is what makes a tap point's stream distinct from
  * the previous stage — without a transformation the Pre point would equal Raw, and without an
  * enrichment the Post point would equal the stage before it, so the picker only offers points
  * that differ.
  */
final case class TapOutput(
  name: String,
  hasEnrichment: Boolean,
  hasTransformation: Boolean,
  /** The `resultEnrichment` Cypher text itself, when the output defines one — the query that
    * produced the data a Post ([[TapPoint.PostEnrichment]]) tap point observes. `None` when
    * there is no enrichment, or the host hasn't populated it.
    */
  enrichmentQuery: Option[String] = None,
  /** The `preEnrichmentTransformation`'s type discriminator (e.g. `"InlineData"`), when the
    * output defines one — names how a Pre ([[TapPoint.PreEnrichment]]) point's data has been
    * reshaped from the raw match envelope (see [[cards.TapCardQuery.transformationNote]]).
    * `None` when there is no transformation, or the host hasn't populated it.
    */
  transformationType: Option[String] = None,
)

/** A standing query the tap picker can offer to tap: its name and its outputs.
  *
  * The panel's decoupled view of the standing-query catalog. The host populates it (fetching
  * via the shared proxy-aware V2 fetch and mapping into this type), so the panel never depends
  * on the landing/v2api client code. Raw is per-SQ; each output yields a Pre point when it has
  * a transformation and a Post point when it is enriched.
  */
final case class TapCatalogEntry(
  sqName: String,
  outputs: List[TapOutput],
  /** The standing query's match pattern (its Cypher text), when known — the query that
    * produced the data a Raw or Pre ([[TapPoint.PreEnrichment]]) tap point observes (a
    * transformation reshapes that data but issues no query of its own).
    */
  patternQuery: Option[String] = None,
)

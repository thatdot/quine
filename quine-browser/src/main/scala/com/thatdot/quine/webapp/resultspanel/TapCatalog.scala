package com.thatdot.quine.webapp.resultspanel

/** One output of a standing query, as the tap picker sees it: its name and whether it defines a
  * `resultEnrichment` query. Enrichment presence is what makes an output's Pre and Post tap points
  * distinct — without it, Pre would equal Post, so the picker offers only Post.
  */
final case class TapOutput(name: String, hasEnrichment: Boolean)

/** A standing query the tap picker can offer to tap: its name and its outputs.
  *
  * The panel's decoupled view of the standing-query catalog. The host populates it (fetching
  * via the shared proxy-aware V2 fetch and mapping into this type), so the panel never depends
  * on the landing/v2api client code. Raw is per-SQ; each output yields a Post point (and a Pre
  * point when the output is enriched).
  */
final case class TapCatalogEntry(sqName: String, outputs: List[TapOutput])

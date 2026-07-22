package com.thatdot.quine.webapp.resultspanel.cards

import scala.scalajs.js

import com.thatdot.quine.routes.QueryLanguage
import com.thatdot.quine.webapp.resultspanel.{
  ResultsContent,
  TapCatalogEntry,
  TapEntry,
  TapPoint,
  TapTarget,
  ViewerState,
}

/** Opaque identity for one card, minted once at creation and stable for the card's
  * lifetime (survives minimize/expand, re-runs, and session restore).
  */
final case class CardId(value: String) extends AnyVal

object CardId {

  /** A fresh id. Scala.js is single-threaded so a monotonic counter plus a random
    * suffix is enough to keep restored (persisted) ids from colliding with fresh ones.
    */
  private var counter: Long = 0L

  def fresh(): CardId = {
    counter += 1
    CardId(s"card-$counter-${js.Math.random().toString.drop(2)}")
  }
}

/** An adhoc query's identity for card dedupe: two submissions are the same query iff
  * their trimmed text matches exactly (leading/trailing whitespace is submission noise;
  * anything else — including internal reformatting — is a different query). At most one
  * adhoc card exists per identity: the run boundary routes a matching submission into
  * the existing card instead of appending (see `CardsStore.lifecycleMods`).
  *
  * The private constructor forces every key through [[QueryKey.of]], so the
  * normalization lives in exactly one place. (`AnyVal` keeps the wrapper allocation-free
  * — at runtime a key is just the underlying `String`.)
  */
final case class QueryKey private (value: String) extends AnyVal

object QueryKey {
  def of(query: String): QueryKey = new QueryKey(query.trim)
}

/** The query that produced the data a tap-table card is showing — the standing query's
  * match pattern for a Raw/Transformed card, or the output's enrichment query for an
  * Enriched one. `label` names which (see [[TapCardQuery.labelFor]]); `query` is its
  * Cypher text. `note` relates the card's rows to the query's results when they differ in
  * shape: a Raw card's table unwraps the wire envelope ([[TapCardQuery.rawEnvelopeNote]]),
  * and a Transformed card's rows are the transformation's output
  * ([[TapCardQuery.transformationNote]]).
  */
final case class TapCardQuery(label: String, query: String, note: Option[String] = None)

object TapCardQuery {

  /** Which query a tap point's data traces back to, by name. A Pre (Transformed) point's
    * rows are the transformation's output, not the match query's; the parenthetical says
    * so, and the note ([[transformationNote]]) spells out the reshaping.
    */
  def labelFor(tapPoint: TapPoint): String = tapPoint match {
    case TapPoint.Raw => "Match query"
    case _: TapPoint.PreEnrichment => "Match query (its results were then transformed)"
    case _: TapPoint.PostEnrichment => "Enrichment query"
  }

  /** Shape note for a Raw (SQ Matches) card: the table unwraps each result's `data` into
    * columns, so the JSON view is where the full wire shape lives.
    */
  val rawEnvelopeNote: String =
    "Table rows show each match's data fields as columns. Switch to the JSON view for the " +
    "full result shape, including the {\"meta\", \"data\"} envelope."

  /** How a Transformed point's data differs in shape from the raw match stream, in plain
    * English, keyed on the transformation's type discriminator. Every known type gets its
    * specific effect spelled out; an unknown (future) type still gets the honest generic
    * statement rather than nothing.
    */
  def transformationNote(transformationType: Option[String]): String = transformationType match {
    case Some("InlineData") =>
      "The InlineData transformation lifts each match's data fields to the top level: " +
        "rows here are the data object alone, without the raw {\"meta\", \"data\"} envelope."
    case Some(other) =>
      s"The $other transformation has reshaped each result: rows here are its output, " +
        "not the raw {\"meta\", \"data\"} match envelope."
    case None =>
      "This output's transformation has reshaped each result: rows here are its output, " +
        "not the raw {\"meta\", \"data\"} match envelope."
  }

  /** The query behind the data `tapPoint` observes on `sq`: the standing query's match
    * pattern for Raw/Pre (a transformation reshapes that data but issues no query of its
    * own), the output's enrichment query for Post. A Raw point carries the
    * [[rawEnvelopeNote]] and a Pre point the [[transformationNote]], each relating the
    * card's rows to the query's results. `None` when the catalog doesn't carry the text
    * (non-Cypher pattern, or an output whose enrichment has since been removed).
    */
  def forPoint(sq: TapCatalogEntry, tapPoint: TapPoint): Option[TapCardQuery] = tapPoint match {
    case TapPoint.Raw =>
      sq.patternQuery.map(TapCardQuery(labelFor(tapPoint), _, Some(rawEnvelopeNote)))
    case TapPoint.PreEnrichment(out) =>
      sq.patternQuery.map { pattern =>
        val transformationType = sq.outputs.find(_.name == out).flatMap(_.transformationType)
        TapCardQuery(labelFor(tapPoint), pattern, Some(transformationNote(transformationType)))
      }
    case TapPoint.PostEnrichment(out) =>
      sq.outputs.find(_.name == out).flatMap(_.enrichmentQuery).map(TapCardQuery(labelFor(tapPoint), _))
  }

  /** [[forPoint]], looked up through a catalog by target. */
  def resolve(catalog: Vector[TapCatalogEntry], target: TapTarget): Option[TapCardQuery] =
    catalog.find(_.sqName == target.sqName).flatMap(forPoint(_, target.tapPoint))

  /** What each tap point observes — the hover tooltip shared by the tap pickers
    * ([[com.thatdot.quine.webapp.resultspanel.AddTapChooser]], the pipeline tree). When the
    * query that produced the data is known, it's appended so the tooltip doubles as a
    * query preview.
    */
  def hoverDesc(tapPoint: TapPoint, queryText: Option[String]): String = {
    val base = tapPoint match {
      case TapPoint.Raw =>
        "SQ Matches: every match the standing query produces, before any output runs. " +
          "Each result is the raw {\"meta\", \"data\"} match envelope."
      case _: TapPoint.PreEnrichment =>
        "Transformed: results after this output's transformation, before its enrichment query. " +
          "Each result is the transformation's output, not the raw {\"meta\", \"data\"} match envelope."
      case _: TapPoint.PostEnrichment =>
        "Enriched: final results after this output's enrichment query has run. " +
          "Each result is the enrichment query's returned columns."
    }
    queryText.fold(base)(q => s"$base\n\n${labelFor(tapPoint)}:\n$q")
  }
}

/** What a card shows. One case per design doc §3 sketch, adapted to the types that
  * actually exist in this codebase:
  *
  *   - [[CardKind.AdhocCard]] — a one-shot query run (or its restored echo). `outcome`
  *     is `None` while the card is waiting on its first/next run (e.g. immediately
  *     after `ReRun`).
  *   - [[CardKind.TapTableCard]] — a live tabular tap, backed by the existing
  *     [[TapEntry]] (which itself wraps a `LiveStream`).
  */
sealed abstract class CardKind
object CardKind {

  final case class AdhocCard(
    query: String,
    language: QueryLanguage,
    outcome: Option[ResultsContent],
  ) extends CardKind

  final case class TapTableCard(
    target: TapTarget,
    entry: TapEntry,
    /** The query behind the data at this tap point, when known — see [[TapCardQuery]].
      * `None` when the host couldn't resolve one (e.g. a non-Cypher standing query pattern).
      */
    query: Option[TapCardQuery],
  ) extends CardKind
}

/** Sample vs. live semantics (design doc §3). Adhoc cards are always [[SampleMode.Sampled]]
  * (no live button for them, per the locked decision); tap cards start sampled and can
  * [[CardCommand.GoLive]].
  */
sealed abstract class SampleMode
object SampleMode {
  case object Sampled extends SampleMode
  case object Live extends SampleMode
}

/** One card: identity, what it shows, and its own display/sampling/stop state. Mirrors
  * the design doc §3 sketch (`id`, `kind`, `title`, `createdAt`, `mode`, stop flag,
  * `viewer` — the sample budget lives in [[ViewerState.sampleSize]] with the other
  * per-card display knobs).
  *
  * @param title truncated query text (adhoc) or tap name/point label (taps) — see
  *              [[CardState.titleFor]]
  * @param createdAt display label for when the card was created, in the same `HH:MM`
  *                  shape as [[com.thatdot.quine.webapp.resultspanel.ResultsStore.nowLabel]]
  *                  (kept as a pre-formatted `String`, not a `js.Date`, so cards restored
  *                  from session storage don't need a `js.Date` codec)
  * @param mode        Sampled | Live — taps only ever show the Live control
  * @param stopped     tap-table cards auto-stop on minimize (design doc §3): the buffer
  *                     is a frozen snapshot and the tap subscription freed. To the UI a
  *                     stopped card is simply *paused* — the same state a filled sample
  *                     budget produces — and leaves it through the paused exits
  *                     (fetch-more / go-live continuation reopens; see
  *                     `CardsStore.stopCard`/`replaceTapTableEntry`). Adhoc cards have
  *                     nothing running to stop, so they never set this flag
  * @param editAssociated when true, the next run submitted from the main query bar
  *                       updates this card instead of spawning a new one (the `Edit ↑`
  *                       association from design doc §3's expanded-card header)
  * @param viewer      per-card table/json/filter/sort/export/sampling state — the existing
  *                    [[ViewerState]], one instance per card instead of the single shared
  *                    one `ResultsStore` used
  */
final case class CardState(
  id: CardId,
  kind: CardKind,
  title: String,
  createdAt: String,
  mode: SampleMode,
  stopped: Boolean,
  editAssociated: Boolean,
  viewer: ViewerState,
)

object CardState {

  /** Truncated display title for a card: the query text (adhoc) or the tap's target
    * label (taps) — see [[TapTarget.label]].
    */
  def titleFor(kind: CardKind): String = kind match {
    case CardKind.AdhocCard(query, _, _) => query.trim.replaceAll("\\s+", " ")
    case CardKind.TapTableCard(target, _, _) => target.label
  }

  /** A fresh adhoc card for a just-submitted query, before its first result arrives. */
  def freshAdhoc(query: String, language: QueryLanguage, createdAt: String): CardState = {
    val kind = CardKind.AdhocCard(query, language, outcome = None)
    CardState(
      id = CardId.fresh(),
      kind = kind,
      title = titleFor(kind),
      createdAt = createdAt,
      mode = SampleMode.Sampled,
      stopped = false,
      editAssociated = false,
      viewer = ViewerState.initial,
    )
  }

  /** A fresh tap-table card for a newly opened tap. `query` is the tap's underlying query
    * (see [[TapCardQuery]]), resolved by the host from the tap catalog at open time.
    */
  def freshTapTable(target: TapTarget, entry: TapEntry, query: Option[TapCardQuery], createdAt: String): CardState = {
    val kind = CardKind.TapTableCard(target, entry, query)
    CardState(
      id = CardId.fresh(),
      kind = kind,
      title = titleFor(kind),
      createdAt = createdAt,
      mode = SampleMode.Sampled, // taps open showing only the first sample budget; `Go live` lifts the cap
      stopped = false,
      editAssociated = false,
      viewer = ViewerState.initial,
    )
  }

  def isAdhoc(kind: CardKind): Boolean = kind match {
    case _: CardKind.AdhocCard => true
    case _ => false
  }
}

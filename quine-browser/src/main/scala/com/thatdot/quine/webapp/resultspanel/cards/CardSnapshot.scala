package com.thatdot.quine.webapp.resultspanel.cards

import com.raquo.laminar.api.L._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, Json}

import com.thatdot.quine.routes.{CypherQueryResult, QueryLanguage}
import com.thatdot.quine.webapp.resultspanel.streaming.LiveStream
import com.thatdot.quine.webapp.resultspanel.{
  LiveSource,
  Provenance,
  ResultOutcome,
  ResultsContent,
  ResultsView,
  SortDir,
  SourceKind,
  SourceStatus,
  TapEntry,
  TapPoint,
  TapTarget,
  ViewerState,
}

/** The persisted subset of a card's [[ViewerState]]. Ephemeral popover state
  * (`filterOpen`/`exportOpen`) and `selectedRow` (row identity is positional and not
  * stable across restores of streaming buffers) are deliberately absent.
  */
final case class CardViewerSnapshot(
  view: String, // "table" | "json"
  csvFlat: Boolean,
  search: String,
  sortCol: Option[Int],
  sortDir: String, // "asc" | "desc"
  colWidths: Vector[Double],
)

/** A persisted adhoc-card result — the full [[ResultOutcome]] payload, flattened to one
  * plain-data shape (a sealed hierarchy would need discriminator-based codecs for no
  * gain; unused fields for a given `kind` are just empty). The retired `error` kind is
  * still decoded (as `Restored`, keeping its message) for snapshots saved before failed
  * runs became toasts instead of cards.
  */
final case class CardOutcomeSnapshot(
  kind: String, // "tabular" | "text" | "empty" | "restored" (+ legacy "error" on decode)
  columns: Seq[String], // tabular | empty
  rows: Seq[Seq[Json]], // tabular
  values: Seq[Json], // text
  wasTabular: Boolean, // empty
  errorMessage: Option[String], // restored
)

/** One card, reduced to plain serializable data for the host's per-tab, per-namespace
  * persistence path ([[com.thatdot.quine.webapp.queryui.ExplorerStore]]). Live objects
  * are projected away: a tap-table card keeps only its [[TapTarget]] coordinates (its
  * stream buffer restarts empty on reconnect), and an adhoc card keeps its query + full
  * last outcome.
  *
  * `stopped` records the card's state at save time; for a tap-table card the *restored*
  * state is always stopped (a frozen placeholder entry) and the host re-opens the tap for
  * cards that were live — see [[CardSnapshot.toState]] and the host's restore wiring.
  */
final case class CardSnapshot(
  id: String,
  kind: String, // "adhoc" | "tapTable"
  title: String,
  createdAt: String,
  sampleSize: Int,
  live: Boolean, // SampleMode.Live
  stopped: Boolean,
  viewer: CardViewerSnapshot,
  // adhoc
  query: String,
  language: String, // "cypher" | "gremlin"
  outcome: Option[CardOutcomeSnapshot],
  // tap kinds
  sqName: String,
  tapPoint: String, // "raw" | "post:<output>"
  tapQueryLabel: Option[String], // "Match query" | "Enrichment query" — see TapCardQuery
  tapQueryText: Option[String],
  // Transformed cards' shape note (TapCardQuery.note); missing in older snapshots → None.
  tapQueryNote: Option[String] = None,
)

object CardSnapshot {

  val KindAdhoc = "adhoc"
  val KindTapTable = "tapTable"

  implicit val viewerEncoder: Encoder[CardViewerSnapshot] = deriveEncoder
  implicit val viewerDecoder: Decoder[CardViewerSnapshot] = deriveDecoder
  implicit val outcomeEncoder: Encoder[CardOutcomeSnapshot] = deriveEncoder
  implicit val outcomeDecoder: Decoder[CardOutcomeSnapshot] = deriveDecoder
  implicit val encoder: Encoder[CardSnapshot] = deriveEncoder
  implicit val decoder: Decoder[CardSnapshot] = deriveDecoder

  // ── string codings for the small enums ──────────────────────────────────────────

  private def encodeTapPoint(tp: TapPoint): String = tp match {
    case TapPoint.Raw => "raw"
    case TapPoint.PreEnrichment(out) => s"pre:$out"
    case TapPoint.PostEnrichment(out) => s"post:$out"
  }

  /** Inverse of [[encodeTapPoint]]. Output names cannot contain `:` (resource names, per
    * AIP-122, forbid colons — same guarantee [[TapTarget.key]] relies on). Unrecognized
    * codings decode to `None` and their cards are dropped on restore.
    */
  private def decodeTapPoint(s: String): Option[TapPoint] =
    if (s == "raw") Some(TapPoint.Raw)
    else if (s.startsWith("pre:")) Some(TapPoint.PreEnrichment(s.stripPrefix("pre:")))
    else if (s.startsWith("post:")) Some(TapPoint.PostEnrichment(s.stripPrefix("post:")))
    else None

  private def encodeLanguage(l: QueryLanguage): String = l match {
    case QueryLanguage.Cypher => "cypher"
    case QueryLanguage.Gremlin => "gremlin"
  }

  private def decodeLanguage(s: String): QueryLanguage = s match {
    case "gremlin" => QueryLanguage.Gremlin
    case _ => QueryLanguage.Cypher
  }

  private def encodeOutcome(outcome: ResultOutcome): CardOutcomeSnapshot = {
    val empty = CardOutcomeSnapshot("", Nil, Nil, Nil, wasTabular = false, errorMessage = None)
    outcome match {
      case ResultOutcome.Tabular(result) =>
        empty.copy(kind = "tabular", columns = result.columns, rows = result.results)
      case ResultOutcome.TextResults(values) => empty.copy(kind = "text", values = values)
      case ResultOutcome.EmptyResult(wasTabular, columns) =>
        empty.copy(kind = "empty", wasTabular = wasTabular, columns = columns)
      case ResultOutcome.Restored(msg) => empty.copy(kind = "restored", errorMessage = msg)
    }
  }

  private def decodeOutcome(snap: CardOutcomeSnapshot): ResultOutcome = snap.kind match {
    case "tabular" => ResultOutcome.Tabular(CypherQueryResult(snap.columns, snap.rows))
    case "text" => ResultOutcome.TextResults(snap.values)
    case "empty" => ResultOutcome.EmptyResult(snap.wasTabular, snap.columns)
    // "restored", plus the legacy "error" kind — restored with its message intact.
    case _ => ResultOutcome.Restored(snap.errorMessage)
  }

  private def encodeViewer(vs: ViewerState): CardViewerSnapshot =
    CardViewerSnapshot(
      view = vs.view.now() match {
        case ResultsView.Json => "json"
        case ResultsView.Table => "table"
      },
      csvFlat = vs.csvFlat.now(),
      search = vs.search.now(),
      sortCol = vs.sortCol.now(),
      sortDir = vs.sortDir.now() match {
        case SortDir.Desc => "desc"
        case SortDir.Asc => "asc"
      },
      colWidths = vs.colWidths.now(),
    )

  /** `sampleSize` arrives separately because it lives at the [[CardSnapshot]] top level
    * (persisted-format compatibility with when it was a `CardState` field) rather than in
    * [[CardViewerSnapshot]].
    */
  private def decodeViewer(snap: CardViewerSnapshot, sampleSize: Int): ViewerState = {
    val vs = ViewerState.initial
    vs.view.set(if (snap.view == "json") ResultsView.Json else ResultsView.Table)
    vs.csvFlat.set(snap.csvFlat)
    vs.search.set(snap.search)
    vs.sortCol.set(snap.sortCol)
    vs.sortDir.set(if (snap.sortDir == "desc") SortDir.Desc else SortDir.Asc)
    vs.colWidths.set(snap.colWidths)
    vs.sampleSize.set(sampleSize.max(1))
    vs
  }

  /** The [[TapTarget]] a tap-kind snapshot points at; `None` for adhoc snapshots or an
    * undecodable tap point.
    */
  def tapTargetOf(snap: CardSnapshot): Option[TapTarget] =
    if (snap.kind == KindTapTable)
      decodeTapPoint(snap.tapPoint).map(TapTarget(snap.sqName, _))
    else None

  // ── projection to/from the live card model ──────────────────────────────────────

  def fromState(c: CardState): CardSnapshot = {
    val base = CardSnapshot(
      id = c.id.value,
      kind = "",
      title = c.title,
      createdAt = c.createdAt,
      sampleSize = c.viewer.sampleSize.now(),
      live = c.mode == SampleMode.Live,
      stopped = c.stopped,
      viewer = encodeViewer(c.viewer),
      query = "",
      language = encodeLanguage(QueryLanguage.Cypher),
      outcome = None,
      sqName = "",
      tapPoint = "",
      tapQueryLabel = None,
      tapQueryText = None,
      tapQueryNote = None,
    )
    c.kind match {
      case CardKind.AdhocCard(query, language, outcome) =>
        base.copy(
          kind = KindAdhoc,
          query = query,
          language = encodeLanguage(language),
          outcome = outcome.map(content => encodeOutcome(content.outcome)),
        )
      case CardKind.TapTableCard(target, _, query) =>
        base.copy(
          kind = KindTapTable,
          sqName = target.sqName,
          tapPoint = encodeTapPoint(target.tapPoint),
          tapQueryLabel = query.map(_.label),
          tapQueryText = query.map(_.query),
          tapQueryNote = query.flatMap(_.note),
        )
    }
  }

  /** A frozen, ended [[TapEntry]] standing in for a restored tap-table card's stream
    * until (and unless) the host re-opens the tap and swaps a live entry in via
    * [[CardsStore.replaceTapTableEntry]].
    */
  private def placeholderEntry(target: TapTarget): TapEntry = {
    val stream = new LiveStream
    stream.freeze()
    val entry = new TapEntry(
      LiveSource(
        id = s"restored:${target.key}",
        provenance = Provenance(SourceKind.Tap, target.label),
        status = Val(SourceStatus.Ended),
        records = EventStream.empty,
        tapTarget = Some(target),
      ),
      stream,
    )
    entry.ended.set(true)
    entry
  }

  /** Rebuild a live [[CardState]] from a persisted snapshot. `None` when the snapshot's
    * kind or tap point no longer decodes (a corrupt, removed-format, or future-format
    * entry — e.g. a "tapGraph" snapshot persisted before graph-tap cards were removed —
    * the card is dropped rather than failing the whole restore).
    *
    *   - Adhoc: the full saved outcome is restored (a mid-run save with no outcome yet
    *     becomes the `Restored` placeholder).
    *   - Tap-table: restored `stopped` with a [[placeholderEntry]]; the host re-opens the
    *     tap for cards that were live at save time (restart protocol).
    */
  def toState(snap: CardSnapshot): Option[CardState] = {
    val kindOpt: Option[CardKind] = snap.kind match {
      case KindAdhoc =>
        val language = decodeLanguage(snap.language)
        // Run identity is session-scoped, not persisted: each restored content gets a
        // fresh runId, so the first real run against the card reads as a different run
        // (resetting stale viewer state in `CardsStore.refreshAdhocCard`).
        val outcome = snap.outcome
          .map(o => ResultsContent(decodeOutcome(o), snap.query, language, ResultsContent.nextRunId()))
          .orElse(Some(ResultsContent(ResultOutcome.Restored(None), snap.query, language, ResultsContent.nextRunId())))
        Some(CardKind.AdhocCard(snap.query, language, outcome))
      case KindTapTable =>
        val query = for {
          label <- snap.tapQueryLabel
          text <- snap.tapQueryText
        } yield TapCardQuery(label, text, snap.tapQueryNote)
        tapTargetOf(snap).map(target => CardKind.TapTableCard(target, placeholderEntry(target), query))
      case _ => None
    }
    kindOpt.map { kind =>
      CardState(
        id = CardId(snap.id),
        kind = kind,
        title = snap.title,
        createdAt = snap.createdAt,
        mode = if (snap.live) SampleMode.Live else SampleMode.Sampled,
        // A restored tap-table stream is a frozen placeholder until the host swaps a
        // live entry in, so the card starts stopped regardless of its saved state.
        stopped = if (snap.kind == KindTapTable) true else snap.stopped,
        editAssociated = false,
        viewer = decodeViewer(snap.viewer, snap.sampleSize),
      )
    }
  }
}

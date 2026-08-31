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
  TapPointQuery,
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
  * The card's [[CardId]] is deliberately absent: ids are session-scoped, minted by
  * `CardsStore` off one counter, so persisting them would let a restored id collide with a
  * freshly minted one. A restore mints new ids, and the *expanded* card — the only card the
  * snapshot needs to point at — is addressed by its position in the list instead (see
  * `CardsStore.restore`).
  *
  * `stopped` records the card's state at save time; for a tap-table card it folds in the
  * entry's `ended` flag, so a tap paused any way at all — user Stop, filled budget, departed
  * source — persists as stopped. The *restored* state is always stopped (a frozen
  * placeholder entry) and the host re-opens the tap only for cards that were actively
  * streaming (sampling mid-fill, or live) — see [[CardSnapshot.toCard]] and the host's
  * restore wiring; a stopped tap is never reactivated by a restore.
  */
final case class CardSnapshot(
  kind: String, // "adhoc" | "tapTable"
  sampleSize: Int,
  hasSampleLimit: Boolean,
  stopped: Boolean,
  viewer: CardViewerSnapshot,
  // adhoc
  query: String,
  language: String, // "cypher" | "gremlin"
  outcome: Option[CardOutcomeSnapshot],
  // tap kinds
  sqName: String,
  tapPoint: String, // "raw" | "post:<output>"
  tapQueryText: Option[String],
  // A Transformed card's transformation type (TapPointQuery.transformation), from which
  // the popup re-derives its shape note on restore.
  tapQueryTransformation: Option[String] = None,
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
    * (persisted-format compatibility with when it was a card-level field) rather than in
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
    * undecodable tap point. Only standing-query taps are ever persisted (see [[fromCard]]).
    */
  def tapTargetOf(snap: CardSnapshot): Option[TapTarget] =
    if (snap.kind == KindTapTable)
      decodeTapPoint(snap.tapPoint).map(TapTarget.StandingQuery(snap.sqName, _))
    else None

  // ── projection to/from the live card model ──────────────────────────────────────

  /** Project a live card to its persisted form, or `None` for a card that must not outlive the
    * session.
    *
    * A background-query tap card is always `None`: the server's relay is torn down once the run
    * terminates (plus a short grace window) and the row buffer is not persisted, so a restored
    * one could only ever be an empty frozen placeholder — strictly worse than the card simply
    * being gone. Standing-query taps do restore, because their source is still producing.
    */
  def fromCard(c: Card): Option[CardSnapshot] = {
    val base = CardSnapshot(
      kind = "",
      sampleSize = c.viewer.sampleSize.now(),
      // adhoc cards have neither a sample mode nor anything running to stop; the tap
      // branch below sets both
      hasSampleLimit = false,
      stopped = false,
      viewer = encodeViewer(c.viewer),
      query = "",
      language = encodeLanguage(QueryLanguage.Cypher),
      outcome = None,
      sqName = "",
      tapPoint = "",
      tapQueryText = None,
      tapQueryTransformation = None,
    )
    c match {
      case AdhocCard(_, query, language, outcome, _, _) =>
        Some(
          base.copy(
            kind = KindAdhoc,
            query = query,
            language = encodeLanguage(language),
            outcome = outcome.map(content => encodeOutcome(content.outcome)),
          ),
        )
      case TapTableCard(_, TapTarget.StandingQuery(sqName, tapPoint), entry, query, stopped, hasSampleLimit, _) =>
        // `ended` folds into `stopped`: a pause is a pause — user Stop, filled budget, and
        // a departed source all persist alike, and none of them may come back streaming on
        // restore.
        val ended = entry.ended.now()
        Some(
          base.copy(
            kind = KindTapTable,
            hasSampleLimit = hasSampleLimit,
            stopped = stopped || ended,
            sqName = sqName,
            tapPoint = encodeTapPoint(tapPoint),
            tapQueryText = query.map(_.query),
            tapQueryTransformation = query.flatMap(_.transformation),
          ),
        )
      case TapTableCard(_, _: TapTarget.BackgroundQuery, _, _, _, _, _) => None
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

  /** Rebuild a live [[Card]] from a persisted snapshot, under the caller-supplied `id`
    * (snapshots carry no id of their own — `CardsStore.restore`, the only caller, mints one
    * from its counter). `None` when the snapshot's
    * kind or tap point no longer decodes (a corrupt, removed-format, or future-format
    * entry — e.g. a "tapGraph" snapshot persisted before graph-tap cards were removed —
    * the card is dropped rather than failing the whole restore).
    *
    *   - Adhoc: the full saved outcome is restored (a mid-run save with no outcome yet
    *     becomes the `Restored` placeholder).
    *   - Tap-table: restored `stopped` with a [[placeholderEntry]]; the host re-opens the
    *     tap only for cards saved actively streaming (`!snap.stopped` — mid-fill sampling
    *     or live; a paused tap stays paused, its sample budget reset to the default since
    *     its buffer restarts empty and nothing will refill it). A reconnecting card keeps
    *     its whole saved budget, which its empty buffer then refills. The restored
    *     card itself always keeps its sample cap regardless of the saved
    *     `hasSampleLimit` — it starts stopped on a placeholder entry, not actually live,
    *     and a reopen that never resolves (SQ deleted, WS down) must not leave a
    *     permanently-stopped card claiming to be live (see `CardsStore.sampleBudgetFor`,
    *     which trusts the flag alone: a stuck uncapped stopped card would hand out an
    *     unbounded budget to whatever taps its target next). The saved value still reaches
    *     the host via [[wasLive]], which drives `CardsStore.restoreLive` — the
    *     successful-reopen path that lifts the cap again once the fresh source arrives.
    */
  def toCard(snap: CardSnapshot, id: CardId): Option[Card] = {
    // The sample budget the restored viewer starts with. A paused tap resets to the
    // default: its buffer restarts empty, no reconnect will refill it, and a later
    // Get-more recomputes the budget from rows-on-screen anyway — carrying the old
    // total forward would just make the next reopen chase a stale target. A tap that
    // reconnects keeps its whole saved budget: the reconnect starts from an empty buffer
    // (see [[placeholderEntry]]; only a *continuation* reopen seeds from the previous
    // session), so the budget is what the refilled card will hold, not a remainder on top
    // of rows that survived.
    val restoredSampleSize =
      if (snap.kind == KindTapTable && snap.stopped) ViewerState.DefaultSampleSize
      else snap.sampleSize
    // Lazy so an undecodable snapshot (the `None` returns below) allocates no viewer.
    lazy val viewer = decodeViewer(snap.viewer, restoredSampleSize)
    snap.kind match {
      case KindAdhoc =>
        val language = decodeLanguage(snap.language)
        // Run identity is session-scoped, not persisted: each restored content gets a
        // fresh runId, so the first real run against the card reads as a different run
        // (resetting stale viewer state in `CardsStore.refreshAdhocCard`).
        val outcome = snap.outcome
          .map(o => ResultsContent(decodeOutcome(o), snap.query, language, ResultsContent.nextRunId()))
          .orElse(Some(ResultsContent(ResultOutcome.Restored(None), snap.query, language, ResultsContent.nextRunId())))
        Some(AdhocCard(id, snap.query, language, outcome, editAssociated = false, viewer))
      case KindTapTable =>
        val query = snap.tapQueryText.map(TapPointQuery(_, snap.tapQueryTransformation))
        // A restored tap-table stream is a frozen placeholder until the host swaps a
        // live entry in, so the card starts stopped and `Sampled` regardless of its saved
        // state.
        tapTargetOf(snap).map(target =>
          TapTableCard(
            id,
            target,
            placeholderEntry(target),
            query,
            stopped = true,
            hasSampleLimit = true,
            viewer,
          ),
        )
      case _ => None
    }
  }

  /** Whether a tap-table snapshot was live at save time — the host's cue (alongside
    * `!snap.stopped`, which triggers the reopen at all) to ask [[CardsStore.restoreLive]]
    * to put the card back in `Live` mode once the reopen this snapshot triggers actually
    * succeeds. See [[toCard]]: the restored card itself always starts `Sampled`.
    */
  def wasLive(snap: CardSnapshot): Boolean = !snap.hasSampleLimit
}

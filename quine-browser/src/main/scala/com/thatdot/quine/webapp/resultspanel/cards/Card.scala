package com.thatdot.quine.webapp.resultspanel.cards

import com.thatdot.quine.routes.QueryLanguage
import com.thatdot.quine.webapp.resultspanel.{ResultsContent, TapEntry, TapPointQuery, TapTarget, ViewerState}

/** A stable identifier for one card. */
final case class CardId(value: Int) extends AnyVal

/** Information associated with a single entry in the [[CardsStore]] in the bottom right
  * of the exploration UI. There are two kinds of cards:
  *   - [[AdhocCard]] — shows the tabular results from running an ad-hoc query
  *   - [[TapTableCard]] — shows the real-time streaming results of a standing query
  *
  * Only summary information about a card is displayed in the entry in the [[CardsStore]].
  * Clicking on the entry expands the card to show the rest of the information.
  */
sealed abstract class Card {

  /** A stable identifier for this card. */
  val id: CardId

  /** This card's name as the user reads it, displayed in the results panel header and in
    * this card's entry in the [[CardsStore]].
    */
  def title: String

  /** The state of the expanded view of this card. */
  def viewer: ViewerState
}

/** @param editAssociated When true, the next query executed from the main query bar
  *                       updates this card instead of spawning a new one. This becomes
  *                       true after the user clicks `Edit ↑`.
  */
final case class AdhocCard(
  id: CardId,
  query: String,
  language: QueryLanguage,
  outcome: Option[ResultsContent],
  editAssociated: Boolean,
  viewer: ViewerState,
) extends Card {
  val title: String = query.trim.replaceAll("\\s+", " ")
}

object AdhocCard {

  /** A fresh adhoc card for a just-submitted query, before its first result arrives. */
  def fresh(id: CardId, query: String, language: QueryLanguage): AdhocCard =
    AdhocCard(
      id = id,
      query = query,
      language = language,
      outcome = None,
      editAssociated = false,
      viewer = ViewerState.initial,
    )
}

/** @param query          The query behind the data at this tap point
  * @param stopped        Whether the tap is paused
  * @param hasSampleLimit Whether this card's table is capped at its sample budget or
  *                       uncapped (live)
  */
final case class TapTableCard(
  id: CardId,
  target: TapTarget,
  entry: TapEntry,
  query: Option[TapPointQuery],
  stopped: Boolean,
  hasSampleLimit: Boolean,
  viewer: ViewerState,
) extends Card {
  val title: String = target.label
}

object TapTableCard {

  /** A fresh tap-table card for a newly opened tap. `query` is the tap's underlying query
    * (see [[TapPointQuery]]), resolved by the host from the tap catalog at open time.
    *
    * A resumable tap opens capped at its first sample budget — it would otherwise stream
    * forever — and `Go live` lifts the cap. A non-resumable one (a background query) opens
    * uncapped instead: it is a single finite run, so a budget wouldn't defer the rest of the
    * results, it would discard them, and the fetch-more that normally recovers them can't
    * reopen the stream. The stream's own row cap still bounds it.
    */
  def fresh(id: CardId, target: TapTarget, entry: TapEntry, query: Option[TapPointQuery]): TapTableCard =
    TapTableCard(
      id,
      target,
      entry,
      query,
      stopped = false,
      hasSampleLimit = target.resumable,
      viewer = ViewerState.initial,
    )
}

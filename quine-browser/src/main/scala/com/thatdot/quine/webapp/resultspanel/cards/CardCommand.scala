package com.thatdot.quine.webapp.resultspanel.cards

import com.thatdot.quine.webapp.resultspanel.ViewerCommand

/** Every state change in the card system, emitted by components and interpreted by
  * [[CardsStore.dispatch]]. Card-scoped viewer changes (sort/filter/export/…) are
  * wrapped in [[CardCommand.OnViewer]], same shape as `ResultsCommand.OnViewer` wraps
  * `ViewerCommand` today — just re-targeted per card instead of at one shared viewer.
  */
sealed abstract class CardCommand
object CardCommand {

  /** Expand this card into the popup; per the one-expanded-at-a-time invariant, any
    * currently-expanded card minimizes first.
    */
  final case class Expand(id: CardId) extends CardCommand

  /** Minimize the expanded card back into the drawer (text cards auto-stop, design §3). */
  final case class Minimize(id: CardId) extends CardCommand

  /** Close (remove) a card entirely: stops its tap (if any) and drops it from the list. */
  final case class Close(id: CardId) extends CardCommand

  /** Re-run an adhoc card's query as-is (its own stored query text, not the main bar's). */
  final case class ReRun(id: CardId) extends CardCommand

  /** Stop a tap card: freezes its `LiveStream` buffer to a static snapshot and frees the
    * tap subscription. The frozen card is *paused*, not dead — [[FetchMoreSamples]] and
    * [[GoLive]] are its two exits, both continuation reopens that keep the rows on screen.
    */
  final case class Stop(id: CardId) extends CardCommand

  /** Drop the sample budget and follow the live stream (tap cards only; design §3). From a
    * frozen stream this reopens the tap as a continuation (rows on screen stay put).
    */
  final case class GoLive(id: CardId) extends CardCommand

  /** Edit this card's batch-size field (`ViewerState.sampleBatch` — the size of the *next*
    * fetch, not the live display cap, and not itself a fetch).
    */
  final case class SetSampleSize(id: CardId, size: Int) extends CardCommand

  /** Request another batch of `sampleBatch` results/matches ("Get N more"). From a frozen
    * stream (budget filled or user Stop, in either mode) this is the "resume sampled" exit:
    * the mode returns to Sampled and the tap reopens as a continuation.
    */
  final case class FetchMoreSamples(id: CardId) extends CardCommand

  /** `Edit ↑` (adhoc cards only): send the card's query text to the main Monaco query bar
    * and mark the card edit-associated, so the next run updates it instead of spawning a
    * new card. Tap cards have no query bar edit — tap definitions are edited via the tap
    * modal — so this command is a no-op for them.
    */
  final case class EditQuery(id: CardId) extends CardCommand

  /** Drawer search text, filtering minimized cards by title/query (design §3 "Overflow"). */
  final case class Search(text: String) extends CardCommand

  /** A card-scoped [[ViewerCommand]] (sort, filter, export, row selection, …), applied
    * to card `id`'s own `ViewerState`.
    */
  final case class OnViewer(id: CardId, cmd: ViewerCommand) extends CardCommand
}

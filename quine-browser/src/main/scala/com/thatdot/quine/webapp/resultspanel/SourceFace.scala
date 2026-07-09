package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles

/** The shared visual identity ("face") of a viewable source, so a query result, an error, and a
  * live tap read as the same family of thing — a *source* — wherever they appear (the header chip,
  * the picker cards, the history rows, the body's left edge). A face is a **kind class** (which sets
  * `--kind-accent` for its subtree in CSS), a small **glyph**, and a short uppercase **label**.
  *
  * Kept in one place so the three kinds stay in lockstep across the surface; change the mapping here
  * and the chip, picker, and body all move together.
  */
object SourceFace {

  /** Kind class for a query run — `Result` (blue) normally, `Error` (red) when it failed. */
  def outcomeClass(outcome: ResultOutcome): String = outcome match {
    case _: ResultOutcome.ErrorResult => Styles.kindError
    case _ => Styles.kindQuery
  }

  def outcomeIcon(outcome: ResultOutcome): HtmlElement = outcome match {
    case _: ResultOutcome.ErrorResult => ResultsIcons.alert
    case _ => ResultsIcons.query
  }

  /** Kind class for a tap (teal) — always the same; its live/ended/error *status* is a separate dot. */
  val tapClass: String = Styles.kindTap

  /** The kind class for whatever source is shown in the Main pane (`Left` = query, `Right` = tap),
    * used to thread the body's left edge to the chip. Empty string when nothing is shown.
    */
  def mainClass(main: Option[Either[ResultsContent, TapEntry]]): String = main match {
    case Some(Left(content)) => outcomeClass(content.outcome)
    case Some(Right(_)) => tapClass
    case None => ""
  }
}

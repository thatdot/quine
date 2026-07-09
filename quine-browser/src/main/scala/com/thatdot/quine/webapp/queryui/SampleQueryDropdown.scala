package com.thatdot.quine.webapp.queryui

import com.raquo.airstream.split.KeyedStrictSignal
import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.Styles

/** The dropdown list of sample queries that appears when the query editor is focused, filtered
  * as the user types, plus the query-normalization used to drive that filtering.
  *
  * Mouse-driven: ArrowUp/ArrowDown and Enter belong to the editor (history recall and
  * run/newline). The open/closed state, highlight, and filtered list are owned by
  * [[MonacoQueryInput]] (the focus handlers there drive them) and passed in.
  */
object SampleQueryDropdown {

  def apply(
    highlightedIdx: Var[Option[Int]],
    filteredSampleQueries: Signal[Seq[(SampleQuery, Int)]],
    updateQuery: String => Unit,
    showSampleQueryDropdown: Var[Boolean],
  ): HtmlElement = div(
    cls := Styles.sampleQueryDropdown,
    display <-- filteredSampleQueries
      .combineWith(showSampleQueryDropdown.signal)
      .map { case (filtered, shown) => if (filtered.nonEmpty && shown) "" else "none" },
    onMouseLeave --> (_ => highlightedIdx.set(None)),
    children <-- filteredSampleQueries.splitSeq(_._2) { signal =>
      item(signal, highlightedIdx, updateQuery, showSampleQueryDropdown)
    },
  )

  /** An item in the sample queries dropdown for selecting a sample query. The row's name/query are
    * bound to `signal` so editing a sample query in place updates the displayed text reactively
    * (the `splitSeq` key is the index, so an in-place edit reuses this element rather than rebuilding it).
    */
  private def item(
    signal: KeyedStrictSignal[Int, (SampleQuery, Int)],
    highlightedIdx: Var[Option[Int]],
    updateQuery: String => Unit,
    showSampleQueryDropdown: Var[Boolean],
  ): HtmlElement = {
    // The original index — the `splitSeq(_._2)` key, constant for this element's lifetime.
    val originalIndex = signal.key
    div(
      cls := Styles.sampleQueryItem,
      cls <-- highlightedIdx.signal.map(hi =>
        if (hi.contains(originalIndex)) Styles.sampleQueryItemHighlighted else "",
      ),
      onMouseEnter --> (_ => highlightedIdx.set(Some(originalIndex))),
      // Keep focus in the editor so selecting an item doesn't blur-close the dropdown mid-click
      onMouseDown --> (_.preventDefault()),
      onClick --> { _ =>
        updateQuery(signal.now()._1.query)
        showSampleQueryDropdown.set(false)
        highlightedIdx.set(None)
      },
      span(cls := Styles.sampleQueryName, child.text <-- signal.map(_._1.name)),
      span(cls := Styles.sampleQueryText, child.text <-- signal.map(_._1.query)),
    )
  }

  /** Lowercases and removes redundant spaces from a Cypher query string. This is used to filter
    * the sample query popup list based on what the user types in the query input.
    * NOTE: This does not correctly handle string literals nor escaped quotes therein.
    */
  def normalizeQueryForFilter(query: String): String = {
    val collapsed = query.toLowerCase.replaceAll(" +", " ")
    val sb = new StringBuilder(collapsed.length)
    for (i <- collapsed.indices) {
      val c = collapsed(i)
      if (
        c != ' ' ||
        (i > 0 && collapsed(i - 1).isLetterOrDigit &&
        i < collapsed.length - 1 && collapsed(i + 1).isLetterOrDigit)
      )
        sb.append(c)
    }
    sb.toString
  }

}

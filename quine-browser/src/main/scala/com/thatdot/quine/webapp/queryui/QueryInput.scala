package com.thatdot.quine.webapp.queryui

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom.KeyboardEvent

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.Styles

/** The query text input, submit/cancel button, and sample queries dropdown. */
object QueryInput {

  def apply(
    query: Signal[String],
    updateQuery: String => Unit,
    runningTextQuery: Signal[Boolean],
    queryBarColor: Signal[Option[String]],
    sampleQueries: Signal[Seq[SampleQuery]],
    submitButton: Boolean => Unit,
    cancelButton: () => Unit,
    canRead: Boolean,
  ): HtmlElement = {
    // This being true is a prerequisite for showing the sample query dropdown.
    val showSampleQueryDropdown: Var[Boolean] = Var(false)

    // The sample queries filtered to match what the user typed, each paired with its index into
    // sampleQueries to serve as a stable key for `splitSeq`.
    val filteredSampleQueries: Var[Seq[(SampleQuery, Int)]] = Var(Seq.empty)

    // The index (into sampleQueries) of the currently highlighted query in the sample query dropdown.
    val highlightedIdx: Var[Option[Int]] = Var(None)

    // True if the mouse has been moved since the last keystroke. This helps dropdown items ignore
    // mouse events while the arrow keys are being used.
    val mouseMovedSinceKeystroke: Var[Boolean] = Var(true)

    // The actual text box that the user types the query into
    val queryInputInput: Input = input(
      typ := "text",
      placeholder := (if (canRead) "Query returning nodes" else "Not Authorized to READ from graph"),
      cls := Styles.queryInputInput,
      cls <-- queryBarColor.map(_.getOrElse("")),
      styleAttr <-- runningTextQuery.map { running =>
        if (running) "animation: activequery 1.5s ease infinite" else ""
      },
      controlled(
        value <-- query,
        onInput.mapToValue --> (v => updateQuery(v)),
      ),
      onKeyDown --> handleKeyDown(
        updateQuery,
        submitButton,
        showSampleQueryDropdown,
        highlightedIdx,
        filteredSampleQueries,
        mouseMovedSinceKeystroke,
      ),
      filteredSampleQueries.signal.updates --> (_ => highlightedIdx.set(None)),
      sampleQueries.combineWith(query).map { case (queries, userInput) =>
        val normalizedUserInput = normalizeQueryForFilter(userInput)
        if (normalizedUserInput.isEmpty) queries.zipWithIndex
        else queries.zipWithIndex.filter(q => normalizeQueryForFilter(q._1.query).contains(normalizedUserInput))
      } --> filteredSampleQueries.writer,
      onFocus --> (_ => showSampleQueryDropdown.set(true)),
      onBlur --> { _ =>
        showSampleQueryDropdown.set(false)
        highlightedIdx.set(None)
      },
      disabled <-- runningTextQuery.map(_ || !canRead),
    )

    div(
      cls := Styles.queryInput,
      styleAttr := "position: relative",
      queryInputInput,
      sampleQueryDropdown(
        highlightedIdx,
        filteredSampleQueries.signal,
        mouseMovedSinceKeystroke,
        updateQuery,
        showSampleQueryDropdown,
        () => queryInputInput.ref.blur(),
      ),
      child <-- runningTextQuery.map { running =>
        button(
          cls := s"${Styles.grayClickable} ${Styles.queryInputButton}",
          onClick --> { e =>
            if (running) cancelButton()
            else submitButton(e.shiftKey)
          },
          title := (if (running) "Cancel query" else "Hold \"Shift\" to return results as a table"),
          disabled := !canRead,
          if (running) "Cancel" else "Query",
        )
      },
    )
  }

  /** Handler for keyDown events on the query text input */
  private def handleKeyDown(
    updateQuery: String => Unit,
    submitButton: Boolean => Unit,
    showSampleQueryDropdown: Var[Boolean],
    highlightedIdx: Var[Option[Int]],
    filteredSampleQueries: Var[Seq[(SampleQuery, Int)]],
    mouseMovedSinceKeystroke: Var[Boolean],
  ): KeyboardEvent => Unit = { e =>
    e.key match {
      case "ArrowDown" =>
        e.preventDefault()
        mouseMovedSinceKeystroke.set(false)
        val filtered = filteredSampleQueries.now()
        val next = highlightedIdx.now() match {
          case None => filtered.headOption.map(_._2)
          case Some(idx) =>
            val pos = filtered.indexWhere(_._2 == idx)
            if (pos < filtered.length - 1) Some(filtered(pos + 1)._2) else Some(idx)
        }
        highlightedIdx.set(next)
      case "ArrowUp" =>
        e.preventDefault()
        mouseMovedSinceKeystroke.set(false)
        val filtered = filteredSampleQueries.now()
        val prev = highlightedIdx.now() match {
          case None => None
          case Some(idx) =>
            val pos = filtered.indexWhere(_._2 == idx)
            if (pos > 0) Some(filtered(pos - 1)._2) else None
        }
        highlightedIdx.set(prev)
      case "Enter" =>
        val hi = highlightedIdx.now()
        if (showSampleQueryDropdown.now() && hi.isDefined) {
          hi.flatMap(idx => filteredSampleQueries.now().find(_._2 == idx)).foreach { case (q, _) =>
            updateQuery(q.query)
            highlightedIdx.set(None)
          }
        } else {
          submitButton(e.shiftKey)
        }
      case _ => ()
    }
  }

  /** The dropdown list of sample queries that appears when the user types in the
    * query text box.
    */
  private def sampleQueryDropdown(
    highlightedIdx: Var[Option[Int]],
    filteredSampleQueries: Signal[Seq[(SampleQuery, Int)]],
    mouseMovedSinceKeystroke: Var[Boolean],
    updateQuery: String => Unit,
    showSampleQueryDropdown: Var[Boolean],
    blurInput: () => Unit,
  ): HtmlElement = div(
    cls := Styles.sampleQueryDropdown,
    display <-- filteredSampleQueries
      .combineWith(showSampleQueryDropdown.signal)
      .map { case (filtered, shown) => if (filtered.nonEmpty && shown) "" else "none" },
    onMouseMove --> (_ => mouseMovedSinceKeystroke.set(true)),
    onMouseLeave --> (_ => highlightedIdx.set(None)),
    children <-- filteredSampleQueries.splitSeq(_._2) { signal =>
      val (q, originalIndex) = signal.now()
      sampleQueryDropdownItem(
        q,
        originalIndex,
        highlightedIdx,
        mouseMovedSinceKeystroke,
        updateQuery,
        showSampleQueryDropdown,
        blurInput,
      )
    },
  )

  /** An item in the sample queries dropdown for selecting a sample query. */
  private def sampleQueryDropdownItem(
    q: SampleQuery,
    originalIndex: Int,
    highlightedIdx: Var[Option[Int]],
    mouseMovedSinceKeystroke: Var[Boolean],
    updateQuery: String => Unit,
    showSampleQueryDropdown: Var[Boolean],
    blurInput: () => Unit,
  ): HtmlElement = div(
    cls := Styles.sampleQueryItem,
    cls <-- highlightedIdx.signal.map(hi => if (hi.contains(originalIndex)) Styles.sampleQueryItemHighlighted else ""),
    onMouseEnter --> (_ => if (mouseMovedSinceKeystroke.now()) highlightedIdx.set(Some(originalIndex))),
    onMouseMove --> (_ => if (mouseMovedSinceKeystroke.now()) highlightedIdx.set(Some(originalIndex))),
    onMountBind { ctx =>
      highlightedIdx.signal --> Observer[Option[Int]] { hi =>
        if (hi.contains(originalIndex))
          ctx.thisNode.ref.asInstanceOf[js.Dynamic].scrollIntoView(js.Dynamic.literal(block = "nearest")): Unit
      }
    },
    onMouseDown --> (_.preventDefault()),
    onClick --> { _ =>
      updateQuery(q.query)
      showSampleQueryDropdown.set(false)
      highlightedIdx.set(None)
      blurInput()
    },
    span(cls := Styles.sampleQueryName, q.name),
    span(cls := Styles.sampleQueryText, q.query),
  )

  /** Lowercases and removes redundant spaces from a Cypher query string. This is used to filter
    * the sample query popup list based on what the user types in the query input.
    * NOTE: This does not correctly handle string literals nor escaped quotes therein.
    */
  private def normalizeQueryForFilter(query: String): String = {
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

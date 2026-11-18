package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.dom

import com.thatdot.quine.routes.UiNode

/** Code for the exploration UI's canvas search, which Cmd+F (macOS) or Ctrl+F (elsewhere) opens.
  * This object contains the rules for which nodes match a search term, the renderers that
  * highlight the matched parts of a piece of text, the node hover tooltip, and the search bar.
  *
  * A node matches a search term when the term occurs, ignoring case, in the node's ID, in one of
  * its labels, in one of its property keys, or in one of its property values (see [[valueText]]).
  */
object CanvasSearch {

  /** CSS class of the search bar's text input. */
  val inputClass = "canvas-search-input"

  /** CSS class of the highlight around each matched part of a piece of text. */
  val markClass = "canvas-search-mark"

  /** Whether `node` matches `term` (see the description of [[CanvasSearch]]). */
  def matches(node: UiNode[String], term: String): Boolean = {
    def hit(text: String): Boolean = matchRanges(text, term).nonEmpty
    term.nonEmpty && (
      hit(node.id) ||
      labelsOf(node).exists(hit) ||
      node.properties.exists { case (k, v) => hit(k) || hit(valueText(v)) }
    )
  }

  /** Returns Laminar nodes that display `text` with each occurrence of `term` highlighted. */
  def highlighted(text: String, term: String): List[Node] =
    pieces(text, term).map {
      case (piece, true) => htmlTag("mark")(cls := markClass, piece)
      case (piece, false) => textToTextNode(piece)
    }

  /** Returns the element that vis-network shows when the mouse rests on `node`. The element lists
    * the node's ID, its host (when `showHost` is true), its labels, and its properties sorted by
    * key, highlighting each occurrence of `searchTerm` in the ID, labels, keys, and values.
    */
  def tooltip(node: UiNode[String], showHost: Boolean, searchTerm: String): dom.HTMLElement = {
    val root = dom.document.createElement("div").asInstanceOf[dom.HTMLElement]

    // Each line is a list of (text, whether to highlight the search term in that text).
    def addLine(bold: Boolean, segments: (String, Boolean)*): Unit = {
      val line = dom.document.createElement(if (bold) "strong" else "div").asInstanceOf[dom.HTMLElement]
      line.style.display = "block"
      segments.foreach { case (text, searchable) =>
        if (searchable) appendHighlighted(line, text, searchTerm)
        else { val _ = line.appendChild(dom.document.createTextNode(text)) }
      }
      val _ = root.appendChild(line)
    }

    addLine(bold = true, "ID : " -> false, node.id -> true)
    if (showHost) addLine(bold = true, s"Served from Host : ${node.hostIndex}" -> false)
    val labels = labelsOf(node)
    if (labels.nonEmpty) {
      val labelSegments = labels.zipWithIndex.flatMap { case (l, i) =>
        (if (i > 0) List(", " -> false) else Nil) :+ (l -> true)
      }
      addLine(bold = true, ("Labels : " -> false) +: labelSegments: _*)
    }
    node.properties.toList.sortBy(_._1).foreach { case (key, json) =>
      val valueSegments = json.asString match {
        case Some(str) => List("\"" -> false, str -> true, "\"" -> false)
        case None => List(json.noSpaces -> true)
      }
      addLine(bold = false, (List(key -> true, " : " -> false) ++ valueSegments): _*)
    }
    root
  }

  /** Returns the search bar shown at the top of the graph canvas.
    *
    * @param termVar the search term, which the text input reads and writes
    * @param resultCount number of nodes that match the search term
    * @param currentPosition zero-based position of the current result among the results, if any
    * @param focusRequests each event focuses the text input and selects its contents
    * @param onNext called when the user asks for the next result (button, Enter, or Down arrow)
    * @param onPrevious called when the user asks for the previous result (button, Shift+Enter, or
    *   Up arrow)
    * @param onClose called when the user closes the bar (button or Escape)
    */
  def bar(
    termVar: Var[String],
    resultCount: Signal[Int],
    currentPosition: Signal[Option[Int]],
    focusRequests: EventStream[Unit],
    onNext: () => Unit,
    onPrevious: () => Unit,
    onClose: () => Unit,
  ): HtmlElement = {
    val countText = termVar.signal.combineWith(resultCount, currentPosition).map {
      case ("", _, _) => ""
      case (_, 0, _) => "No results"
      case (_, n, Some(p)) => s"${p + 1} of $n"
      case (_, n, None) => if (n == 1) "1 result" else s"$n results"
    }
    val noResults = resultCount.map(_ == 0)

    def iconButton(label: String, iconClass: String, onPress: () => Unit, disable: Signal[Boolean]) =
      button(
        tpe := "button",
        cls := "canvas-search-btn",
        aria.label := label,
        title := label,
        disabled <-- disable,
        htmlTag("i")(cls := iconClass),
        onClick --> { _ => onPress() },
      )

    div(
      cls := "canvas-search-bar",
      input(
        cls := inputClass,
        tpe := "text",
        placeholder := "Search nodes",
        aria.label := "Search nodes by ID, label, or property",
        controlled(
          value <-- termVar.signal,
          onInput.mapToValue --> termVar,
        ),
        onKeyDown --> { e =>
          if (e.key == "Enter") {
            e.preventDefault()
            if (e.shiftKey) onPrevious() else onNext()
          } else if (e.key == "ArrowDown") {
            e.preventDefault()
            onNext()
          } else if (e.key == "ArrowUp") {
            e.preventDefault()
            onPrevious()
          } else if (e.key == "Escape") {
            e.preventDefault()
            onClose()
          }
        },
        onMountCallback { ctx =>
          ctx.thisNode.ref.focus()
          ctx.thisNode.ref.select()
        },
        inContext(el => focusRequests --> { _ => el.ref.focus(); el.ref.select() }),
      ),
      span(cls := "canvas-search-count", child.text <-- countText),
      iconButton("Previous result", "ion-ios-arrow-thin-up", onPrevious, noResults),
      iconButton("Next result", "ion-ios-arrow-thin-down", onNext, noResults),
      iconButton("Close search", "ion-ios-close-empty", onClose, Val(false)),
    )
  }

  /** Returns the labels of `node`. The server sets the `label` of a node without labels to
    * "ID: " followed by the node's ID, so such a node has no labels.
    */
  private def labelsOf(node: UiNode[String]): Seq[String] =
    if (node.label.isEmpty || node.label.startsWith("ID: ")) Nil
    else node.label.split(":").toSeq.filter(_.nonEmpty)

  /** Returns the text that the search matches against for a property value: a string's contents,
    * or any other value as compact JSON.
    */
  private def valueText(json: Json): String = json.asString.getOrElse(json.noSpaces)

  /** Returns the `(start, end)` ranges (end exclusive) where `term` occurs in `text`, ignoring
    * case, from left to right and without overlaps. An empty `term` occurs nowhere.
    */
  private def matchRanges(text: String, term: String): List[(Int, Int)] =
    if (term.isEmpty) Nil
    else {
      val ranges = List.newBuilder[(Int, Int)]
      var i = 0
      while (i + term.length <= text.length)
        if (text.regionMatches(true, i, term, 0, term.length)) {
          ranges += ((i, i + term.length))
          i += term.length
        } else i += 1
      ranges.result()
    }

  /** Splits `text` into the pieces before, between, and after the occurrences of `term`. Returns
    * each piece paired with whether it is an occurrence.
    */
  private def pieces(text: String, term: String): List[(String, Boolean)] = {
    val (rest, end) = matchRanges(text, term).foldLeft((List.empty[(String, Boolean)], 0)) {
      case ((acc, from), (start, stop)) =>
        val withPlain = if (start > from) (text.substring(from, start), false) :: acc else acc
        ((text.substring(start, stop), true) :: withPlain, stop)
    }
    val all = if (end < text.length) (text.substring(end), false) :: rest else rest
    all.reverse
  }

  /** Appends `text` to `parent`, with each occurrence of `term` highlighted. */
  private def appendHighlighted(parent: dom.Node, text: String, term: String): Unit =
    pieces(text, term).foreach {
      case (piece, true) =>
        val m = dom.document.createElement("mark")
        m.classList.add(markClass)
        m.textContent = piece
        parent.appendChild(m)
      case (piece, false) =>
        parent.appendChild(dom.document.createTextNode(piece))
    }
}

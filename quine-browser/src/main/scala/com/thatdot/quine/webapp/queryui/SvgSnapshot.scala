package com.thatdot.quine.webapp.queryui

import scala.concurrent.{Future, Promise}
import scala.scalajs.js
import scala.scalajs.js.typedarray.{ArrayBuffer, Uint8Array}

import org.scalajs.dom

import com.thatdot.quine.webapp.components.VisData
import com.thatdot.quine.webapp.queryui.QueryUiVisNodeExt
import com.thatdot.{visnetwork => vis}

/** Produces an SVG DOM element that is a snapshot of the current vis.js graph state.
  *
  * Builds raw DOM SVG elements directly, since the SVG is only used for
  * download (not displayed in the UI tree).
  */
object SvgSnapshot {

  private val SvgNs = "http://www.w3.org/2000/svg"

  private def svgEl(tag: String, attrs: (String, Any)*): dom.Element = {
    val el = dom.document.createElementNS(SvgNs, tag)
    attrs.foreach { case (k, v) => el.setAttribute(k, v.toString) }
    el
  }

  private def textNode(text: String): dom.Text = dom.document.createTextNode(text)

  /** Given a `vis` graph, produce an SVG element that is a snapshot of its current state.
    *
    * The icon font is fetched, base64-encoded, and embedded as a `@font-face` declaration
    * so that icons render correctly without requiring the viewer to have the font installed.
    *
    * @param graphData data from the `vis` graph
    * @param positions location of nodes in the graph
    * @param edgeColor the color for edges
    * @param fontFace  CSS font-family name used by vis-network for node icons
    * @param fontUrl   URL to fetch the icon font file for embedding
    */
  def apply(
    graphData: VisData,
    positions: js.Dictionary[vis.Position],
    edgeColor: String = "#2b7ce9",
    fontFace: String = "Ionicons",
    fontUrl: String = "fonts/ionicons.woff",
  ): Future[dom.Element] = {
    val promise = Promise[dom.Element]()

    val fontFormat = fontFormatHint(fontUrl)
    val request = new dom.XMLHttpRequest()
    request.open("GET", fontUrl, async = true)
    request.responseType = "arraybuffer"
    request.onload = _ =>
      promise.success {
        val fontBase64 = if (request.status == 200) {
          val buffer = request.response.asInstanceOf[ArrayBuffer]
          Some(arrayBufferToBase64(buffer))
        } else {
          println(s"Failed to load font from $fontUrl (status ${request.status}), icons won't be preserved")
          None
        }
        makeSnapshot(graphData, positions, edgeColor, fontFace, fontBase64, fontFormat)
      }
    request.onerror = _ =>
      promise.success {
        println(s"Failed to load font from $fontUrl, icons won't be preserved")
        makeSnapshot(graphData, positions, edgeColor, fontFace, None, fontFormat)
      }
    request.send()

    promise.future
  }

  private def arrayBufferToBase64(buffer: ArrayBuffer): String = {
    val bytes = new Uint8Array(buffer)
    val binary = new StringBuilder(bytes.length)
    for (i <- 0 until bytes.length)
      binary.append(bytes(i).toChar)
    dom.window.btoa(binary.toString())
  }

  // Ionicons (and most icon fonts) use the Unicode Private Use Area for their glyphs.
  // Non-PUA characters are emoji or standard Unicode (e.g. π) that aren't in the icon
  // font; we render them with an explicit `serif` fallback (see makeNodeIcon).
  private def isPrivateUseArea(code: String): Boolean =
    code.nonEmpty && {
      val cp = code.codePointAt(0)
      (cp >= 0xE000 && cp <= 0xF8FF) || (cp >= 0xF0000 && cp <= 0x10FFFF)
    }

  private def fontFormatHint(url: String): String = {
    val lower = url.toLowerCase
    if (lower.endsWith(".woff2")) "woff2"
    else if (lower.endsWith(".ttf")) "truetype"
    else if (lower.endsWith(".otf")) "opentype"
    else "woff"
  }

  /** Given the graph data, positions, and optionally an embedded font, construct an SVG element */
  private def makeSnapshot(
    graphData: VisData,
    positions: js.Dictionary[vis.Position],
    edgeColor: String,
    fontFace: String,
    fontBase64: Option[String],
    fontFormat: String,
  ): dom.Element = {
    val elements = Seq.newBuilder[dom.Element]

    // Circle defs keyed by (color, size) for nodes without icons or when font is unavailable
    val circlesUsed = collection.mutable.Map.empty[(String, Double), String]
    val definitions = Seq.newBuilder[dom.Element]

    // Embed the icon font via @font-face so icons render without external dependencies
    fontBase64.foreach { base64 =>
      val style = svgEl("style")
      style.appendChild(
        textNode(
          s"@font-face { font-family: '$fontFace'; src: url(data:font/$fontFormat;base64,$base64) format('$fontFormat'); }",
        ),
      )
      definitions += style
    }

    // Define what arrow heads look like
    val arrowMarker = svgEl(
      "marker",
      "id" -> "arrowhead",
      "markerWidth" -> "14",
      "markerHeight" -> "10",
      "refX" -> "40",
      "refY" -> "5",
      "orient" -> "auto",
    )
    val arrowPolygon = svgEl("polygon", "fill" -> edgeColor, "points" -> "0 0, 14 5, 0 10, 2 5")
    arrowMarker.appendChild(arrowPolygon)
    definitions += arrowMarker

    // Define outline for text
    val outlineFilter = svgEl("filter", "id" -> "outlined")
    outlineFilter.appendChild(
      svgEl("feMorphology", "in" -> "SourceAlpha", "result" -> "DILATED", "operator" -> "dilate", "radius" -> "1"),
    )
    outlineFilter.appendChild(
      svgEl("feFlood", "flood-color" -> "white", "flood-opacity" -> "1", "result" -> "FLOODED"),
    )
    outlineFilter.appendChild(
      svgEl("feComposite", "in" -> "FLOODED", "in2" -> "DILATED", "operator" -> "in", "result" -> "OUTLINE"),
    )
    val feMerge = svgEl("feMerge")
    feMerge.appendChild(svgEl("feMergeNode", "in" -> "OUTLINE"))
    feMerge.appendChild(svgEl("feMergeNode", "in" -> "SourceGraphic"))
    outlineFilter.appendChild(feMerge)
    definitions += outlineFilter

    /** Construct a text label where the text is centered and has a white outline */
    def makeLabel(cX: Double, cY: Double, lbl: String): dom.Element = {
      val t = svgEl("text", "x" -> cX, "y" -> cY, "style" -> "filter: url(#outlined)", "text-anchor" -> "middle")
      t.appendChild(textNode(lbl))
      t
    }

    def iconTextEl(code: String, fontFamily: String, size: Double, color: String): dom.Element = {
      val el = svgEl(
        "text",
        "font-family" -> fontFamily,
        "font-size" -> size,
        "fill" -> color,
        "text-anchor" -> "middle",
        "dominant-baseline" -> "central",
      )
      el.appendChild(textNode(code))
      el
    }

    /** Construct a node icon: a font glyph `<text>` if the font is embedded and an icon code
      * is available, otherwise a fallback circle.
      */
    def makeNodeIcon(
      cx: Double,
      cy: Double,
      nodeSize: Option[Double],
      iconCode: Option[String],
      nodeColor: Option[String],
      tooltip: String,
    ): dom.Element = {
      val color = nodeColor.getOrElse("#97c2fc")
      val size = nodeSize.getOrElse(30.0)

      val g = svgEl("g", "transform" -> s"translate($cx $cy)")

      iconCode match {
        case Some(code) if isPrivateUseArea(code) && fontBase64.isDefined =>
          g.appendChild(iconTextEl(code, s"'$fontFace'", size, color))

        // Emoji and standard Unicode (e.g. π) aren't in the icon font, so on the vis-network
        // canvas the browser silently falls back to a system font to draw them. We can't name
        // that per-glyph fallback, and letting the SVG viewer pick its own would make the export
        // diverge from the canvas (and from itself, across viewers). Hardcoding `serif` gives a
        // stable, predictable glyph everywhere the SVG is opened — the case where the character's
        // font actually matters visually.
        case Some(code) if !isPrivateUseArea(code) =>
          g.appendChild(iconTextEl(code, "serif", size, color))

        case _ =>
          val refSvgId = circlesUsed.getOrElseUpdate(
            (color, size), {
              val defId = s"circle-${(size, color).hashCode.abs}"
              definitions += svgEl(
                "circle",
                "id" -> defId,
                "fill" -> "rgba(0,0,0,0)",
                "stroke-width" -> (size / 10),
                "stroke" -> color,
                "r" -> (size / 2.6),
              )
              defId
            },
          )
          g.appendChild(svgEl("use", "href" -> s"#$refSvgId"))
      }

      val titleEl = svgEl("title")
      titleEl.appendChild(textNode(tooltip))
      g.appendChild(titleEl)
      g
    }

    // Draw edges first
    for {
      edge <- graphData.edgeSet.get()
      from <- positions.get(edge.from.asInstanceOf[String])
      to <- positions.get(edge.to.asInstanceOf[String])
    } {
      elements += svgEl(
        "line",
        "x1" -> from.x,
        "y1" -> from.y,
        "x2" -> to.x,
        "y2" -> to.y,
        "marker-end" -> "url(#arrowhead)",
        "stroke" -> edgeColor,
      )
      for (lbl <- edge.label)
        elements += makeLabel((from.x + to.x) / 2, (from.y + to.y) / 2, lbl)
    }

    // Draw nodes second
    for (node <- graphData.nodeSet.get()) {
      val queryUiNode = node.asInstanceOf[QueryUiVisNodeExt]
      val pos = positions(queryUiNode.id.asInstanceOf[String])

      val properties = queryUiNode.uiNode.properties.toVector
        .sortBy(_._1)
        .map { case (k, v) => s"$k: ${v.noSpaces}" }
        .mkString("\u000A")

      elements += makeNodeIcon(
        pos.x,
        pos.y,
        node.icon.toOption.flatMap(_.size.toOption),
        node.icon.toOption.flatMap(_.code.toOption),
        node.icon.toOption.flatMap(_.color.toOption),
        properties,
      )
      for (lbl <- node.label)
        elements += makeLabel(pos.x, pos.y + 30, lbl)
    }

    // Estimate a viewBox
    val maxX = positions.values.map(_.x).max + 50
    val minX = positions.values.map(_.x).min - 50
    val maxY = positions.values.map(_.y).max + 50
    val minY = positions.values.map(_.y).min - 50

    val svgHeight = maxY - minY
    val svgWidth = maxX - minX

    val svgRoot = svgEl(
      "svg",
      "width" -> s"${svgWidth}px",
      "height" -> s"${svgHeight}px",
      "viewBox" -> s"$minX $minY $svgWidth $svgHeight",
      "version" -> "1.1",
      "font-family" -> "Arial",
      "xmlns" -> SvgNs,
    )

    val defsEl = svgEl("defs")
    definitions.result().foreach(defsEl.appendChild)
    svgRoot.appendChild(defsEl)

    elements.result().foreach(svgRoot.appendChild)

    svgRoot
  }
}

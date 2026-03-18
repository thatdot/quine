package com.thatdot.quine.webapp.queryui

import scala.concurrent.{Future, Promise}
import scala.scalajs.js
import scala.util.{Failure, Success, Try}

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

  /** Given a `vis` graph, produce an SVG element that is a snapshot of its current state
    *
    * @param graphData data from the `vis` graph
    * @param positions location of nodes in the graph
    * @param edgeColor the color for edges
    * @param svgFont name of the SVG font to use
    */
  def apply(
    graphData: VisData,
    positions: js.Dictionary[vis.Position],
    edgeColor: String = "#2b7ce9",
    svgFont: String = "ionicons.svg",
  ): Future[dom.Element] = {
    val promise = Promise[dom.Element]()

    val request = new dom.XMLHttpRequest()
    request.open("GET", svgFont, async = true)
    request.onload = _ =>
      promise.success {
        val icons: Map[String, Glyph] = Try(extractGlyphs(request.responseXML)) match {
          case Success(glyphs) => glyphs
          case Failure(err) =>
            println(s"Failed to load SVG font, icons won't be preserved $err")
            Map.empty
        }

        makeSnapshot(graphData, positions, edgeColor, icons)
      }
    request.send()

    promise.future
  }

  /** Hacked up representation of the parts of `<glyph>` we care about */
  private case class Glyph(
    name: String,
    d: String,
    width: Double,
    height: Double,
  )

  /** Pull out a mapping of unicode character to SVG glyph */
  private def extractGlyphs(svgFont: dom.Document): Map[String, Glyph] = {
    val charAdvanceX = svgFont.getElementsByTagName("font").apply(0).getAttribute("horiz-adv-x")
    val charAscent = svgFont.getElementsByTagName("font-face").apply(0).getAttribute("ascent")

    svgFont
      .getElementsByTagName("glyph")
      .iterator
      .map { glyph =>
        val parsed = Glyph(
          glyph.getAttribute("glyph-name"),
          glyph.getAttribute("d"),
          Option(glyph.getAttribute("horiz-adv-x")).getOrElse(charAdvanceX).toDouble,
          charAscent.toDouble,
        )
        glyph.getAttribute("unicode") -> parsed
      }
      .toMap
  }

  /** Given the graph data, positions, and SVG icon paths, construct an SVG element */
  private def makeSnapshot(
    graphData: VisData,
    positions: js.Dictionary[vis.Position],
    edgeColor: String,
    iconGlyphs: Map[String, Glyph],
  ): dom.Element = {
    val elements = Seq.newBuilder[dom.Element]

    // Map from `(icon, color, size)` to name of the `def`
    val iconsUsed = collection.mutable.Map.empty[(Option[Glyph], String, Double), String]
    val definitions = Seq.newBuilder[dom.Element]

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

    /** Construct an icon for a node, by creating or re-using a definition */
    def makeNodeIcon(
      cx: Double,
      cy: Double,
      nodeSize: Option[Double],
      nodeGlyph: Option[Glyph],
      nodeColor: Option[String],
      tooltip: String,
    ): dom.Element = {
      val color = nodeColor.getOrElse("#97c2fc")
      val size = nodeSize.getOrElse(30.0)

      val refSvgId: String = iconsUsed.getOrElseUpdate(
        (nodeGlyph, color, size),
        nodeGlyph match {
          case Some(Glyph(name, dPath, width, height)) =>
            val defId = s"icon-${(size, color).hashCode.abs}-$name"
            val scale = size / height
            val gDef = svgEl("g", "id" -> defId)
            gDef.appendChild(
              svgEl(
                "path",
                "d" -> dPath,
                "fill" -> color,
                "transform" -> s"scale($scale -$scale) translate(-${width / 2} -${height / 2})",
              ),
            )
            definitions += gDef
            "#" + defId

          case None =>
            val defId = s"circle-${(size, color).hashCode.abs}"
            definitions += svgEl(
              "circle",
              "id" -> defId,
              "fill" -> "rgba(0,0,0,0)",
              "stroke-width" -> (size / 10),
              "stroke" -> color,
              "r" -> (size / 2.6),
            )
            "#" + defId
        },
      )

      val g = svgEl("g", "transform" -> s"translate($cx $cy)")
      g.appendChild(svgEl("use", "href" -> refSvgId))
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
        node.icon.toOption.flatMap(_.code.toOption).flatMap(iconGlyphs.get(_)),
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

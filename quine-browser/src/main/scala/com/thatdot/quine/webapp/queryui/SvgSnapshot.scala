package com.thatdot.quine.webapp.queryui

import scala.concurrent._
import scala.scalajs.js
import scala.scalajs.js.Dynamic.{literal => jsObj}
import scala.util.{Failure, Success, Try}

import org.scalajs.dom
import slinky.core.CustomAttribute
import slinky.core.facade.ReactElement
import slinky.web.svg._

import com.thatdot.quine.webapp.components.VisData
import com.thatdot.{visnetwork => vis}

object SvgSnapshot {

  /** Given a `vis` graph, produce an SVG of that is a snapshot its current
    * state
    *
    *   - node's icon color, size, and shape are preserved, with the shape being
    *     inlined (so the SVG is lean and dependency free)
    *
    *   - the SVG produced will be sized to include all nodes on the canvas and
    *     to match the default on-screen sizes (before a user zooms)
    *
    * TODO: handle clusters
    *
    * @param graphData data from the `vis` graph
    * @param positions location of nodes in the graph (from [[getPositions]])
    * @param edgeColor the color for edges
    * @param svgFont name of the SVG font to use
    */
  def apply(
    graphData: VisData,
    positions: js.Dictionary[vis.Position],
    edgeColor: String = "#2b7ce9",
    svgFont: String = "ionicons.svg"
  ): Future[ReactElement] = {
    val promise = Promise[ReactElement]()

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
  final private case class Glyph(
    name: String,
    d: String,
    width: Double,
    height: Double
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
          charAscent.toDouble
        )
        glyph.getAttribute("unicode") -> parsed
      }
      .toMap
  }

  private val href = CustomAttribute[String]("href")
  private val xmlns = CustomAttribute[String]("xmlns")

  /** Given the graph data, positions, and SVG icon paths, construct and SVG */
  private def makeSnapshot(
    graphData: VisData,
    positions: js.Dictionary[vis.Position],
    edgeColor: String,
    iconGlyphs: Map[String, Glyph]
  ): ReactElement = {
    val elements = Seq.newBuilder[ReactElement]

    // Map from `(icon, color, size)` to name of the `def`
    val iconsUsed = collection.mutable.Map.empty[(Option[Glyph], String, Double), String]
    val definitions = Seq.newBuilder[ReactElement]

    // Define what arrow heads look like
    definitions += marker(
      id := "arrowhead",
      markerWidth := "14",
      markerHeight := "10",
      refX := "40",
      refY := "5",
      orient := "auto"
    )(
      polygon(
        fill := edgeColor,
        points := "0 0, 14 5, 0 10, 2 5"
      )
    )

    // Define outline for text
    definitions += filter(id := "outlined")(
      feMorphology(in := "SourceAlpha", result := "DILATED", operator := "dilate", radius := "1"),
      feFlood(floodColor := "white", floodOpacity := "1", result := "FLOODED"),
      feComposite(in := "FLOODED", in2 := "DILATED", operator := "in", result := "OUTLINE"),
      feMerge()(feMergeNode(in := "OUTLINE"), feMergeNode(in := "SourceGraphic"))
    )

    /* Construct a text label where the text is centered and has a whiet outline */
    def makeLabel(cX: Double, cY: Double, lbl: String) =
      text(x := cX, y := cY, style := jsObj(filter = "url(#outlined)"), textAnchor := "middle")(lbl)

    /* Construct an icon for a node, by creating or re-using a definition
     *
     * @param cx x-position of the center of the node
     * @param cy y-position of the center of the node
     * @param nodeSize size of the node
     * @param iconGlyph glyph of the node
     * @param iconColor color of the node
     * @param tooltip text to show when hovering over the node
     */
    def makeNodeIcon(
      cx: Double,
      cy: Double,
      nodeSize: Option[Double],
      nodeGlyph: Option[Glyph],
      nodeColor: Option[String],
      tooltip: String
    ): ReactElement = {
      val color = nodeColor.getOrElse("#97c2fc")
      val size = nodeSize.getOrElse(30.0)

      val refSvgId: String = iconsUsed.getOrElseUpdate(
        (nodeGlyph, color, size),
        nodeGlyph match {
          case Some(Glyph(name, dPath, width, height)) =>
            val defId = s"icon-${(size, color).hashCode.abs}-$name"
            val scale = size / height
            definitions += g(id := defId)(
              path(
                d := dPath,
                fill := color,
                transform := s"scale($scale -$scale) translate(-${width / 2} -${height / 2})"
              )
            )
            "#" + defId

          case None =>
            val defId = s"circle-${(size, color).hashCode.abs}"
            definitions += circle(
              id := defId,
              fill := "rgba(0,0,0,0)", // unlike `none`, this still brings up the tooltip
              strokeWidth := (size / 10),
              stroke := color,
              r := (size / 2.6)
            )()
            "#" + defId
        }
      )

      g(transform := s"translate($cx $cy)")(
        use(href := refSvgId),
        title(tooltip)
      )
    }

    // Draw edges first
    for {
      edge <- graphData.edgeSet.get()
      from <- positions.get(edge.from.asInstanceOf[String])
      to <- positions.get(edge.to.asInstanceOf[String])
    } {
      elements += line(
        x1 := from.x,
        y1 := from.y,
        x2 := to.x,
        y2 := to.y,
        markerEnd := "url(#arrowhead)",
        stroke := edgeColor
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
        properties
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
    val definitionElement: ReactElement = defs(definitions.result())

    svg(
      width := s"${svgWidth}px",
      height := s"${svgHeight}px",
      viewBox := s"$minX $minY $svgWidth $svgHeight",
      version := "1.1",
      fontFamily := "Arial",
      xmlns := "http://www.w3.org/2000/svg"
    )(
      (definitionElement +: elements.result()): _*
    )
  }
}

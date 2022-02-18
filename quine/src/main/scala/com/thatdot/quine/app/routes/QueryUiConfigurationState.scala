package com.thatdot.quine.app.routes

import scala.concurrent.Future
import scala.io.{Codec, Source}
import scala.util.Try
import scala.util.matching.Regex

import com.thatdot.quine.routes.{SampleQuery, UiNodeAppearance, UiNodeQuickQuery}

object QueryUiConfigurationState {
  val codepointRegex: Regex = raw"(?:\\|&#x|\\u)?([a-f0-9]+);?".r
  // map of full ion- icon name to rendered unicode icon
  val icons: Map[String, String] = Source
    .fromResource("ionicons.tsv")(Codec.UTF8)
    .getLines()
    .map(_.split("\t"))
    .collect { case Array(name, rendered) => (name -> rendered.trim) }
    .toMap

  /** Given a node appearance, return a copy of that appearance where the icon specified (if any)
    * is rendered to a unicode string. The icon may be specified by its ionicons v2 name or a
    * hex codepoint prefixed by either \\ or \\u, or hex-escaped as an HTML character
    * @example a node with icon = Some("cash") => an otherwise-identical node with icon = Some("")
    * @example a node with icon = Some("&amp;#xF11F;") => an otherwise-identical node with icon = Some("&#xF11F;")
    * @param node a node with an icon specification
    * @return a node with a rendered unicode icon
    */
  def renderNodeIcons(node: UiNodeAppearance): UiNodeAppearance = node.copy(
    icon = node.icon match {
      case Some(namedWithPrefix) if namedWithPrefix.startsWith("ion") => icons.get(namedWithPrefix)
      case Some(named) if icons.contains("ion-" + named) => icons.get("ion-" + named)
      case Some(codepointRegex(codepointHex)) =>
        Try(Integer.parseInt(codepointHex, 16).toChar.toString).toOption
      case other => other
    }
  )
}

trait QueryUiConfigurationState {

  def getStartingQueries: Future[Vector[SampleQuery]]

  def getQuickQueries: Future[Vector[UiNodeQuickQuery]]

  def getNodeAppearances: Future[Vector[UiNodeAppearance]]

  def setSampleQueries(newSampleQueries: Vector[SampleQuery]): Future[Unit]

  def setQuickQueries(newQuickQueries: Vector[UiNodeQuickQuery]): Future[Unit]

  def setNodeAppearances(newNodeAppearances: Vector[UiNodeAppearance]): Future[Unit]

}

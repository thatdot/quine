package com.thatdot.quine.webapp.queryui

/** Capability interface for a graph visualization renderer. */
trait GraphVisualization {
  def pinNode(nodeId: String): Unit
  def unpinNode(nodeId: String): Unit
  def unpinNodeWithFlash(nodeId: String): Unit
  def setNodePosition(nodeId: String, x: Double, y: Double): Unit

  /** Temporarily unfix a pinned node so it can be dragged, without removing the pin visual */
  def unfixForDrag(nodeId: String): Unit

  /** Read current node positions. Pin state is tracked separately by [[PinTracker]]. */
  def readNodePositions(): Map[String, (Double, Double)]
}

/** User interaction events from the visualization layer, in domain vocabulary. */
sealed abstract class GraphVisualizationEvent
object GraphVisualizationEvent {
  final case class NodesMoved(nodeIds: Seq[String]) extends GraphVisualizationEvent
  final case class UnpinRequested(nodeIds: Seq[String]) extends GraphVisualizationEvent
}

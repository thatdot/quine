package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._

/** Tracks which nodes are pinned, syncing visual state through [[GraphVisualization]]. */
final class PinTracker(visualization: GraphVisualization) {

  private val pinnedVar: Var[Set[String]] = Var(Set.empty)

  val pinned: Signal[Set[String]] = pinnedVar.signal

  def current: Set[String] = pinnedVar.now()

  def isPinned(nodeId: String): Boolean = pinnedVar.now().contains(nodeId)

  def beginDrag(nodeIds: Iterable[String]): Unit =
    nodeIds.filter(isPinned).foreach(visualization.unfixForDrag)

  def pin(nodeIds: Iterable[String]): Unit = {
    nodeIds.foreach(visualization.pinNode)
    pinnedVar.update(_ ++ nodeIds)
  }

  def unpinWithFlash(nodeIds: Iterable[String]): Unit = {
    nodeIds.foreach(visualization.unpinNodeWithFlash)
    pinnedVar.update(_ -- nodeIds)
  }

  /** Bulk-set pin state (e.g., snapshot restore). Bypasses visualization sync. */
  def resetStateOnly(pinned: Set[String]): Unit = pinnedVar.set(pinned)

  def removeNodes(nodeIds: Iterable[String]): Unit =
    pinnedVar.update(_ -- nodeIds)
}

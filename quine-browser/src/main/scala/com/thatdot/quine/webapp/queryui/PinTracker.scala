package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._

/** Tracks which nodes are pinned, syncing visual state through [[GraphVisualization]]. */
final class PinTracker(visualization: GraphVisualization) {

  private val pinnedVar: Var[Set[String]] = Var(Set.empty)

  val pinned: Signal[Set[String]] = pinnedVar.signal

  def isPinned(nodeId: String): Boolean = pinnedVar.now().contains(nodeId)

  def pin(nodeIds: Iterable[String]): Unit = {
    val current = pinnedVar.now()
    val toAdd = nodeIds.filterNot(current.contains)
    if (toAdd.nonEmpty) pinnedVar.update(_ ++ toAdd)
    nodeIds.foreach(visualization.pinNode)
  }

  def beginDrag(nodeIds: Iterable[String]): Unit =
    nodeIds.filter(isPinned).foreach(visualization.unfixForDrag)

  def unpinWithFlash(nodeIds: Iterable[String]): Unit = {
    val current = pinnedVar.now()
    val toRemove = nodeIds.filter(current.contains)
    if (toRemove.nonEmpty) {
      pinnedVar.update(_ -- toRemove)
      toRemove.foreach(visualization.unpinNodeWithFlash)
    }
  }

  /** Bulk-set pin state (e.g., history replay). Bypasses visualization sync. */
  def resetStateOnly(pinned: Set[String]): Unit = pinnedVar.set(pinned)

  def removeNodes(nodeIds: Iterable[String]): Unit =
    pinnedVar.update(_ -- nodeIds)
}

package com.thatdot

import scala.scalajs.js

import js.|

package object visnetwork {
  type IdType = String | Double

  type DirectionType = String
  object DirectionType {
    val from: DirectionType = "from"
    val to: DirectionType = "to"
  }

  type TimelineAnimationType = Boolean | AnimationOptions

  /** See [[https://visjs.github.io/vis-network/docs/network/#Events]] */
  type NetworkEvents = String
  object NetworkEvents {
    val click: NetworkEvents = "click"
    val doubleClick: NetworkEvents = "doubleClick"
    val oncontext: NetworkEvents = "oncontext"
    val hold: NetworkEvents = "hold"
    val release: NetworkEvents = "release"
    val select: NetworkEvents = "select"
    val selectNode: NetworkEvents = "selectNode"
    val selectEdge: NetworkEvents = "selectEdge"
    val deselectNode: NetworkEvents = "deselectNode"
    val deselectEdge: NetworkEvents = "deselectEdge"
    val dragStart: NetworkEvents = "dragStart"
    val dragging: NetworkEvents = "dragging"
    val dragEnd: NetworkEvents = "dragEnd"
    val hoverNode: NetworkEvents = "hoverNode"
    val blurNode: NetworkEvents = "blurNode"
    val hoverEdge: NetworkEvents = "hoverEdge"
    val blurEdge: NetworkEvents = "blurEdge"
    val zoom: NetworkEvents = "zoom"
    val showPopup: NetworkEvents = "showPopup"
    val hidePopup: NetworkEvents = "hidePopup"
    val startStabilizing: NetworkEvents = "startStabilizing"
    val stabilizationProgress: NetworkEvents = "stabilizationProgress"
    val stabilizationIterationsDone: NetworkEvents = "stabilizationIterationsDone"
    val stabilized: NetworkEvents = "stabilized"
    val resize: NetworkEvents = "resize"
    val initRedraw: NetworkEvents = "initRedraw"
    val beforeDrawing: NetworkEvents = "beforeDrawing"
    val afterDrawing: NetworkEvents = "afterDrawing"
    val animationFinished: NetworkEvents = "animationFinished"
    val configChange: NetworkEvents = "configChange"

  }

  type EasingFunction = String
  object EasingFunctions {
    val linear: EasingFunction = "linear"
    val easeInQuad: EasingFunction = "easeInQuad"
    val easeOutQuad: EasingFunction = "easeOutQuad"
    val easeInOutQuad: EasingFunction = "easeInOutQuad"
    val easeInCubic: EasingFunction = "easeInCubic"
    val easeOutCubic: EasingFunction = "easeOutCubic"
    val easeInOutCubic: EasingFunction = "easeInOutCubic"
    val easeInQuart: EasingFunction = "easeInQuart"
    val easeOutQuart: EasingFunction = "easeOutQuart"
    val easeInOutQuart: EasingFunction = "easeInOutQuart"
    val easeInQuint: EasingFunction = "easeInQuint"
    val easeOutQuint: EasingFunction = "easeOutQuint"
    val easeInOutQuint: EasingFunction = "easeInOutQuint"
  }

  implicit class NetworkOps(network: Network) {
    import NetworkEvents._

    def onClick(callback: ClickEvent => Unit): Unit =
      network.on(click, e => callback(e.asInstanceOf[ClickEvent]))
    def onDoubleClick(callback: ClickEvent => Unit): Unit =
      network.on(doubleClick, e => callback(e.asInstanceOf[ClickEvent]))
    def onContext(callback: ClickEvent => Unit): Unit =
      network.on(oncontext, e => callback(e.asInstanceOf[ClickEvent]))

    def onHold(callback: ClickEvent => Unit): Unit =
      network.on(hold, e => callback(e.asInstanceOf[ClickEvent]))
    def onRelease(callback: ClickEvent => Unit): Unit =
      network.on(release, e => callback(e.asInstanceOf[ClickEvent]))

    def onSelect(callback: ClickEvent => Unit): Unit =
      network.on(select, e => callback(e.asInstanceOf[ClickEvent]))
    def onSelectNode(callback: ClickEvent => Unit): Unit =
      network.on(selectNode, e => callback(e.asInstanceOf[ClickEvent]))
    def onSelectEdge(callback: ClickEvent => Unit): Unit =
      network.on(selectEdge, e => callback(e.asInstanceOf[ClickEvent]))

    def onDragStart(callback: ClickEvent => Unit): Unit =
      network.on(dragStart, e => callback(e.asInstanceOf[ClickEvent]))
    def onDragging(callback: ClickEvent => Unit): Unit =
      network.on(dragging, e => callback(e.asInstanceOf[ClickEvent]))
    def onDragEnd(callback: ClickEvent => Unit): Unit =
      network.on(dragEnd, e => callback(e.asInstanceOf[ClickEvent]))

    def onDeselectNode(callback: DeselectEvent => Unit): Unit =
      network.on(deselectNode, e => callback(e.asInstanceOf[DeselectEvent]))
    def onDeselectEdge(callback: DeselectEvent => Unit): Unit =
      network.on(deselectEdge, e => callback(e.asInstanceOf[DeselectEvent]))

    def onHoverNode(callback: HoverNodeEvent => Unit): Unit =
      network.on(hoverNode, e => callback(e.asInstanceOf[HoverNodeEvent]))
    def onBlurNode(callback: HoverNodeEvent => Unit): Unit =
      network.on(blurNode, e => callback(e.asInstanceOf[HoverNodeEvent]))

    def onHoverEdge(callback: HoverEdgeEvent => Unit): Unit =
      network.on(hoverEdge, e => callback(e.asInstanceOf[HoverEdgeEvent]))
    def onBlurEdge(callback: HoverEdgeEvent => Unit): Unit =
      network.on(blurEdge, e => callback(e.asInstanceOf[HoverEdgeEvent]))

    def onZoom(callback: ZoomEvent => Unit): Unit =
      network.on(zoom, e => callback(e.asInstanceOf[ZoomEvent]))

    def onShowPopup(callback: IdType => Unit): Unit =
      network.on(showPopup, e => callback(e.asInstanceOf[IdType]))
    def onHidePopup(callback: () => Unit): Unit =
      network.on(hidePopup, _ => callback())
  }
}

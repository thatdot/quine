package com.thatdot.visnetwork

import scala.scalajs.js

import js.|

trait Pointer extends js.Object {
  val DOM: Position
  val canvas: Position
}

trait InteractionEvent extends js.Object {
  val nodes: js.Array[IdType]
  val edges: js.Array[IdType]
  val event: js.Object
  val pointer: Pointer
}

trait ClickEvent extends InteractionEvent {
  val items: js.Array[ClickEvent.NodeItem | ClickEvent.EdgeItem]
}
object ClickEvent {
  trait NodeItem extends js.Object {
    val nodeId: IdType
  }
  trait EdgeItem extends js.Object {
    val edgeId: IdType
  }
}

trait DeselectEvent extends InteractionEvent {
  val previousSelection: DeselectEvent.PreviousSelection
}
object DeselectEvent {
  trait PreviousSelection extends js.Object {
    val nodes: js.Array[IdType]
    val edges: js.Array[IdType]
  }
}

trait ControlNodeDragging extends InteractionEvent {
  val controlEdge: ControlNodeDragging.ControlEdge
}
object ControlNodeDragging {
  trait ControlEdge extends js.Object {
    val from: IdType
    val to: IdType
  }
}

trait HoverNodeEvent extends js.Object {
  val nodeId: IdType
}

trait HoverEdgeEvent extends js.Object {
  val edgeId: IdType
}

trait ZoomEvent extends js.Object {
  val direction: String
  val scale: Double
  val pointer: Position
}

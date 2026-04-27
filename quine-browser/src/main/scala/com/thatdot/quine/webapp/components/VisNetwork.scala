package com.thatdot.quine.webapp.components

import scala.scalajs.js
import scala.scalajs.js.JSConverters._

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.{visnetwork => vis}

/** Data types for the vis.js network graph */
final case class Node(id: Int, label: String) { self =>
  def asVis: js.Object with vis.Node = new vis.Node {
    override val id = self.id
    override val label = self.label
  }
}
final case class Edge(id: String, from: Int, to: Int) { self =>
  def asVis: js.Object with vis.Edge = new vis.Edge {
    override val from = self.from
    override val to = self.to
    override val id = self.id
    override val label = self.id
  }
}

/** Wrapper around [[vis.Data]] */
final case class VisData(
  raw: vis.Data,
  nodeSet: vis.DataSet[vis.Node],
  edgeSet: vis.DataSet[vis.Edge],
)
object VisData {
  def apply(nodes: Seq[Node], edges: Seq[Edge]): VisData = {
    val nodeSet = new vis.DataSet(nodes.map(_.asVis).toJSArray)
    val edgeSet = new vis.DataSet(edges.map(_.asVis).toJSArray)

    val raw = new vis.Data {
      override val nodes = nodeSet
      override val edges = edgeSet
    }
    new VisData(raw, nodeSet, edgeSet)
  }
}

/** Several `vis` underlying events have this structure */
trait VisIndirectMouseEvent extends js.Object {
  val srcEvent: dom.MouseEvent
}

/** Laminar wrapper around the vis.js network visualization.
  *
  * On mount, creates a `vis.Network` instance attached to the container div.
  * On unmount, stores positions and destroys the network.
  */
object VisNetwork {

  /** @param data mutable data store backing the network
    * @param afterNetworkInit called with the network object once it is initialized
    * @param options options with which to initialize the network
    */
  def apply(
    data: VisData,
    afterNetworkInit: vis.Network => Unit = _ => (),
    clickHandler: dom.MouseEvent => Unit = _ => (),
    contextMenuHandler: dom.MouseEvent => Unit = _ => (),
    mouseMoveHandler: dom.MouseEvent => Unit = _ => (),
    keyDownHandler: dom.KeyboardEvent => Unit = _ => (),
    options: vis.Network.Options = new vis.Network.Options {},
  ): HtmlElement = {
    var networkOpt: Option[vis.Network] = None

    div(
      position := "absolute",
      top := "0",
      height := "100%",
      width := "100%",
      tabIndex := 0,
      onClick --> (e => clickHandler(e)),
      onMouseMove --> (e => mouseMoveHandler(e)),
      onContextMenu --> (e => contextMenuHandler(e)),
      onKeyDown --> (e => keyDownHandler(e)),
      onMountCallback { ctx =>
        // Defer so the browser has calculated layout and the container has
        // non-zero dimensions. vis.js binds a one-shot resize handler that
        // sets the view translation from the container size; if it fires
        // at 0x0 the coordinate origin lands at the upper-left instead of center.
        val _ = dom.window.requestAnimationFrame { (_: Double) =>
          val network = new vis.Network(ctx.thisNode.ref, data.raw, options)
          networkOpt = Some(network)
          afterNetworkInit(network)
        }
      },
      onUnmountCallback { _ =>
        for (network <- networkOpt) {
          network.storePositions()
          network.destroy()
        }
        networkOpt = None
      },
    )
  }
}

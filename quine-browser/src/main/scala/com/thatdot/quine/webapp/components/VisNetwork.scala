package com.thatdot.quine.webapp.components

import scala.scalajs.js

import org.scalajs.dom
import org.scalajs.dom.html
import slinky.core._
import slinky.core.annotations.react
import slinky.core.facade.{React, ReactElement, ReactRef}
import slinky.web.html._
import slinky.web.{SyntheticKeyboardEvent, SyntheticMouseEvent}

import com.thatdot.{visnetwork => vis}

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
  edgeSet: vis.DataSet[vis.Edge]
)
object VisData {
  def apply(nodes: Seq[Node], edges: Seq[Edge]): VisData = {
    import js.JSConverters._

    val nodeSet = new vis.DataSet(nodes.map(_.asVis).toJSArray)
    val edgeSet = new vis.DataSet(edges.map(_.asVis).toJSArray)

    val raw = new vis.Data {
      override val nodes = nodeSet
      override val edges = edgeSet
    }
    new VisData(raw, nodeSet, edgeSet)
  }
}

/** Navigation bar that sits on the LHS of the window */
@react class VisNetwork extends StatelessComponent {

  /** @param data mutable data store backing the network
    * @param afterNetworkInit called with the network object once it is initialized
    * @param options options with which to intialize the network
    */
  case class Props(
    data: VisData,
    afterNetworkInit: vis.Network => Unit = _ => (),
    onClick: SyntheticMouseEvent[dom.HTMLDivElement] => Unit = _ => (),
    onContextMenu: SyntheticMouseEvent[dom.HTMLDivElement] => Unit = _ => (),
    onMouseMove: SyntheticMouseEvent[dom.HTMLDivElement] => Unit = _ => (),
    onKeyDown: SyntheticKeyboardEvent[dom.HTMLDivElement] => Unit = _ => (),
    options: vis.Network.Options = new vis.Network.Options {}
  )

  // Prepare a reference (to which the vis.js network will be attached)
  val networkRef: ReactRef[html.Div] = React.createRef[html.Div]
  var networkOpt: Option[vis.Network] = None

  override def componentWillUnmount(): Unit = {
    for (network <- networkOpt) {
      network.storePositions()
      network.destroy()
    }
    networkOpt = None
  }

  /* Only _after_ the component has been attached to the DOM can we make the
   * vis.js network (since `this.networkRef.current` now refers to an actual
   * DOM element).
   */
  override def componentDidMount(): Unit = {
    val network = new vis.Network(networkRef.current, props.data.raw, props.options)
    networkOpt = Some(network)
    props.afterNetworkInit(network)
  }

  private val divStyle = js.Dynamic.literal(
    height = "100%",
    width = "100%",
    position = "absolute",
    top = "0"
  )
  def render(): ReactElement = div(
    style := divStyle,
    ref := networkRef,
    onClick := (e => props.onClick(e)),
    onMouseMove := (e => props.onMouseMove(e)),
    onContextMenu := (e => props.onContextMenu(e)),
    onKeyDown := (e => props.onKeyDown(e))
  )
}

/* Several `vis` underlying events have this structure */
trait VisIndirectMouseEvent extends js.Object {
  val srcEvent: dom.MouseEvent
}

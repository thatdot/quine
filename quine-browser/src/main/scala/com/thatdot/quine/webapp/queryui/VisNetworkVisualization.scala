package com.thatdot.quine.webapp.queryui

import org.scalajs.dom.window

import com.thatdot.quine.webapp.components.VisData
import com.thatdot.{visnetwork => vis}

/** vis-network implementation of [[GraphVisualization]].
  * Maps pinned state to vis-network's `fixed` and `shadow` node properties.
  */
final class VisNetworkVisualization(
  graphData: VisData,
  network: () => Option[vis.Network],
) extends GraphVisualization {

  override def pinNode(nodeId: String): Unit = {
    graphData.nodeSet.update(new vis.Node {
      override val id = nodeId
      override val fixed = true
      override val shadow = true
    })
    ()
  }

  override def unpinNode(nodeId: String): Unit = {
    graphData.nodeSet.update(new vis.Node {
      override val id = nodeId
      override val fixed = false
      override val shadow = false
    })
    ()
  }

  override def unpinNodeWithFlash(nodeId: String): Unit = {
    val visNode: vis.Node = graphData.nodeSet.get(nodeId).merge
    graphData.nodeSet.update(new vis.Node {
      override val id = nodeId
      override val fixed = false
      override val shadow = false
      override val icon = new vis.NodeOptions.Icon {
        override val color = "black"
      }
    })
    val originalIcon = visNode.icon
    window.setTimeout(
      () =>
        graphData.nodeSet.update(new vis.Node {
          override val id = nodeId
          override val icon = originalIcon
        }),
      500,
    )
    ()
  }

  override def setNodePosition(nodeId: String, x: Double, y: Double): Unit =
    network().foreach(_.moveNode(nodeId, x, y))

  override def unfixForDrag(nodeId: String): Unit = {
    graphData.nodeSet.update(new vis.Node {
      override val id = nodeId
      override val fixed = false
    })
    ()
  }

  override def readNodePositions(): Map[String, (Double, Double)] = {
    val result = Map.newBuilder[String, (Double, Double)]
    network().foreach { net =>
      for ((nodeId, pos) <- net.getPositions(graphData.nodeSet.getIds()))
        result += nodeId -> (pos.x, pos.y)
    }
    result.result()
  }
}

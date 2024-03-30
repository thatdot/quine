package com.thatdot.visnetwork

import scala.annotation.nowarn
import scala.scalajs.js

import org.scalajs.dom

import js.annotation._
import js.|

/** See [[https://visjs.github.io/vis-network/docs/network/#methods]] */
@js.native
@JSGlobal("vis.Network")
class Network(container: dom.HTMLElement, data: Data, options: Network.Options) extends js.Object {
  def destroy(): Unit = js.native
  def setData(data: Data): Unit = js.native
  def setOptions(options: Network.Options): Unit = js.native
  def on(eventName: NetworkEvents, callback: js.Function1[js.Any, Unit]): Unit = js.native
  @nowarn
  def off(eventName: NetworkEvents, callback: js.Function1[js.Any, Unit] = js.native): Unit = js.native
  def once(eventName: NetworkEvents, callback: js.Function1[js.Any, Unit]): Unit = js.native
  def canvasToDOM(position: Position): Position = js.native
  def DOMtoCanvas(position: Position): Position = js.native
  def redraw(): Unit = js.native
  def setSize(width: String, height: String): Unit = js.native
  @nowarn
  def cluster(options: ClusterOptions = js.native): Unit = js.native
  @nowarn
  def clusterByConnection(nodeId: String, options: ClusterOptions = js.native): Unit = js.native
  @nowarn
  def clusterByHubsize(hubsize: Double = js.native, options: ClusterOptions = js.native): Unit = js.native
  @nowarn
  def clusterOutliers(options: ClusterOptions = js.native): Unit = js.native
  def findNode(nodeId: IdType): js.Array[IdType] = js.native
  def getClusteredEdges(baseEdgeId: IdType): js.Array[IdType] = js.native
  def getBaseEdge(clusteredEdgeId: IdType): IdType = js.native
  def getBaseEdges(clusteredEdgeId: IdType): js.Array[IdType] = js.native
  @nowarn
  def updateEdge(startEdgeId: IdType, options: EdgeOptions = js.native): Unit = js.native
  @nowarn
  def updateClusteredNode(clusteredNodeId: IdType, options: NodeOptions = js.native): Unit = js.native
  def isCluster(nodeId: IdType): Boolean = js.native
  def getNodesInCluster(clusterNodeId: IdType): js.Array[IdType] = js.native
  @nowarn
  def openCluster(nodeId: IdType, options: OpenClusterOptions = js.native): Unit = js.native
  def getSeed(): Double = js.native
  def enableEditMode(): Unit = js.native
  def disableEditMode(): Unit = js.native
  def addNodeMode(): Unit = js.native
  def editNode(): Unit = js.native
  def addEdgeMode(): Unit = js.native
  def editEdgeMode(): Unit = js.native
  def deleteSelected(): Unit = js.native
  @nowarn
  def getPositions(nodeIds: js.Array[IdType] = js.native): js.Dictionary[Position] = js.native
  def getPositions(nodeId: IdType): Position = js.native
  def storePositions(): Unit = js.native
  def moveNode(nodeId: IdType, x: Double, y: Double): Unit = js.native
  def getBoundingBox(nodeId: IdType): BoundingBox = js.native
  @nowarn
  def getConnectedNodes(
    nodeOrEdgeId: IdType,
    direction: DirectionType = js.native
  ): js.Array[IdType] | js.Array[Network.ConnectedEdges] = js.native
  def getConnectedEdges(nodeId: IdType): js.Array[IdType] = js.native
  def startSimulation(): Unit = js.native
  def stopSimulation(): Unit = js.native
  @nowarn
  def stabilize(iterations: Double = js.native): Unit = js.native
  def getSelection(): Network.Selection = js.native
  def getSelectedNodes(): js.Array[IdType] = js.native
  def getSelectedEdges(): js.Array[IdType] = js.native
  def getNodeAt(position: Position): js.UndefOr[IdType] = js.native
  def getEdgeAt(position: Position): IdType = js.native
  @nowarn
  def selectNodes(nodeIds: js.Array[IdType], highlightEdges: Boolean = js.native): Unit = js.native
  def selectEdges(edgeIds: js.Array[IdType]): Unit = js.native
  @nowarn
  def setSelection(selection: Network.Selection, options: SelectionOptions = js.native): Unit = js.native
  def unselectAll(): Unit = js.native
  def getScale(): Double = js.native
  def getViewPosition(): Position = js.native
  @nowarn
  def fit(options: FitOptions = js.native): Unit = js.native
  @nowarn
  def focus(nodeId: IdType, options: FocusOptions = js.native): Unit = js.native
  def moveTo(options: MoveToOptions): Unit = js.native
  def releaseNode(): Unit = js.native
  def getOptionsFromConfigurator(): js.Dynamic = js.native
}
object Network {

  /** See [[https://visjs.github.io/vis-network/docs/network/#options]] */
  trait Options extends js.Object {
    val autoResize: js.UndefOr[Boolean] = js.undefined
    val width: js.UndefOr[String] = js.undefined
    val height: js.UndefOr[String] = js.undefined
    val clickToUse: js.UndefOr[Boolean] = js.undefined
    val configure: js.UndefOr[Options.Configure] = js.undefined
    val edges: js.UndefOr[EdgeOptions] = js.undefined
    val nodes: js.UndefOr[NodeOptions] = js.undefined
    val groups: js.UndefOr[Options.Groups] = js.undefined
    val layout: js.UndefOr[Options.Layout] = js.undefined
    val interaction: js.UndefOr[Options.Interaction] = js.undefined
    val manipulation: js.UndefOr[js.Any] = js.undefined
    val physics: js.UndefOr[Options.Physics] = js.undefined
  }

  object Options {

    /** See [[https://visjs.github.io/vis-network/docs/network/configure.html]] */
    trait Configure extends js.Object {
      val enabled: js.UndefOr[Boolean] = js.undefined
      val filter: js.UndefOr[js.Any] = js.undefined
      val container: js.UndefOr[js.Any] = js.undefined
      val showButton: js.UndefOr[Boolean] = js.undefined
    }

    /** See [[https://visjs.github.io/vis-network/docs/network/groups.html]] */
    trait Groups extends js.Object {
      val useDefaultGroups: js.UndefOr[Boolean] = js.undefined
    }

    /** See [[https://visjs.github.io/vis-network/docs/network/layout.html]] */
    trait Layout extends js.Object {
      val randomSeed: js.UndefOr[Double] = js.undefined
      val improvedLayout: js.UndefOr[Boolean] = js.undefined
      val clusterThreshold: js.UndefOr[Int] = js.undefined
      val hierarchical: js.UndefOr[js.Any] = js.undefined
    }

    /** See [[https://visjs.github.io/vis-network/docs/network/interaction.html]] */
    trait Interaction extends js.Object {
      val dragNodes: js.UndefOr[Boolean] = js.undefined
      val dragView: js.UndefOr[Boolean] = js.undefined
      val hideEdgesOnDrag: js.UndefOr[Boolean] = js.undefined
      val hideEdgesOnZoom: js.UndefOr[Boolean] = js.undefined
      val hideNodesOnDrag: js.UndefOr[Boolean] = js.undefined
      val hover: js.UndefOr[Boolean] = js.undefined
      val hoverConnectedEdges: js.UndefOr[Boolean] = js.undefined
      val keyboard: js.UndefOr[js.Any] = js.undefined
      val multiselect: js.UndefOr[Boolean] = js.undefined
      val navigationButtons: js.UndefOr[Boolean] = js.undefined
      val selectable: js.UndefOr[Boolean] = js.undefined
      val selectConnectedEdges: js.UndefOr[Boolean] = js.undefined
      val tooltipDelay: js.UndefOr[Int] = js.undefined
      val zoomView: js.UndefOr[Boolean] = js.undefined
      val zoomSpeed: js.UndefOr[Double] = js.undefined
    }

    /** See [[https://visjs.github.io/vis-network/docs/network/physics.html]] */
    trait Physics extends js.Object {
      val enabled: js.UndefOr[Boolean] = js.undefined
      val barnesHut: js.UndefOr[Physics.BarnesHut] = js.undefined
      val forceAtlas2Based: js.UndefOr[Physics.ForceAtlas2Based] = js.undefined
      val repulsion: js.UndefOr[Physics.Repulsion] = js.undefined
      val hierarchicalRepulsion: js.UndefOr[Physics.HierarchicalRepulsion] = js.undefined
      val maxVelocity: js.UndefOr[Double] = js.undefined
      val minVelocity: js.UndefOr[Double] = js.undefined
      val solver: js.UndefOr[String] = js.undefined
      val stabilization: js.UndefOr[Physics.Stabilization | Boolean] = js.undefined
      val timestep: js.UndefOr[Double] = js.undefined
      val adaptiveTimestep: js.UndefOr[Boolean] = js.undefined
      val wind: js.UndefOr[Physics.Wind] = js.undefined

    }

    object Physics {

      trait BarnesHut extends js.Object {
        val theta: js.UndefOr[Double] = js.undefined
        val gravitationalConstant: js.UndefOr[Double] = js.undefined
        val centralGravity: js.UndefOr[Double] = js.undefined
        val springLength: js.UndefOr[Double] = js.undefined
        val springConstant: js.UndefOr[Double] = js.undefined
        val damping: js.UndefOr[Double] = js.undefined
        val avoidOverlap: js.UndefOr[Double] = js.undefined
      }

      trait ForceAtlas2Based extends js.Object {
        val theta: js.UndefOr[Double] = js.undefined
        val gravitationalConstant: js.UndefOr[Double] = js.undefined
        val centralGravity: js.UndefOr[Double] = js.undefined
        val springLength: js.UndefOr[Double] = js.undefined
        val springConstant: js.UndefOr[Double] = js.undefined
        val damping: js.UndefOr[Double] = js.undefined
        val avoidOverlap: js.UndefOr[Double] = js.undefined
      }

      trait Repulsion extends js.Object {
        val nodeDistance: js.UndefOr[Double] = js.undefined
        val centralGravity: js.UndefOr[Double] = js.undefined
        val springLength: js.UndefOr[Double] = js.undefined
        val springConstant: js.UndefOr[Double] = js.undefined
        val damping: js.UndefOr[Double] = js.undefined
      }

      trait HierarchicalRepulsion extends js.Object {
        val nodeDistance: js.UndefOr[Double] = js.undefined
        val centralGravity: js.UndefOr[Double] = js.undefined
        val springLength: js.UndefOr[Double] = js.undefined
        val springConstant: js.UndefOr[Double] = js.undefined
        val damping: js.UndefOr[Double] = js.undefined
        val avoidOverlap: js.UndefOr[Double] = js.undefined
      }

      trait Stabilization extends js.Object {
        val enabled: js.UndefOr[Boolean] = js.undefined
        val iterations: js.UndefOr[Int] = js.undefined
        val updateInterval: js.UndefOr[Int] = js.undefined
        val onlyDynamicEdges: js.UndefOr[Boolean] = js.undefined
        val fit: js.UndefOr[Boolean] = js.undefined
      }

      trait Wind extends js.Object {
        val x: Double
        val y: Double
      }
    }
  }

  trait ConnectedEdges extends js.Object {
    val fromId: IdType
    val toId: IdType
  }

  trait Selection extends js.Object {
    val nodes: js.Array[IdType]
    val edges: js.Array[IdType]
  }
}

trait FocusOptions extends ViewPortOptions {
  val locked: js.UndefOr[Boolean] = js.undefined
}

trait ViewPortOptions extends js.Object {
  val scale: js.UndefOr[Double] = js.undefined
  val offset: js.UndefOr[Position] = js.undefined
  val animation: js.UndefOr[AnimationOptions | Boolean] = js.undefined
}

trait MoveToOptions extends ViewPortOptions {
  val position: js.UndefOr[Position] = js.undefined
}

trait AnimationOptions extends js.Object {
  val duration: Double
  val easingFunction: EasingFunction
}

trait FitOptions extends js.Object {
  val nodes: js.UndefOr[js.Array[String]] = js.undefined
  val animation: TimelineAnimationType
}

trait SelectionOptions extends js.Object {
  val unselectAll: js.UndefOr[Boolean] = js.undefined
  val highlightEdges: js.UndefOr[Boolean] = js.undefined
}

trait BoundingBox extends js.Object {
  val top: Double
  val left: Double
  val right: Double
  val bottom: Double
}

trait ClusterOptions extends js.Object {

  /** Function argument: `nodeOptions` */
  val joinCondition: js.UndefOr[js.Function1[js.Any, Boolean]] = js.undefined

  /** Function arguments: `clusterOptions`, `childNodesOptions`, `childEdgesOptions` */
  val processProperties: js.UndefOr[js.Function3[js.Any, js.Array[js.Any], js.Array[js.Any], js.Any]] = js.undefined
  val clusterNodeProperties: js.UndefOr[NodeOptions] = js.undefined
  val clusterEdgeProperties: js.UndefOr[EdgeOptions] = js.undefined
}

trait OpenClusterOptions extends js.Object {
  def releaseFunction(
    clusterPosition: Position,
    containedNodesPositions: js.Dictionary[Position]
  ): js.Dictionary[Position]
}

trait Position extends js.Object {
  val x: Double
  val y: Double
}

trait Data extends js.Object {
  val nodes: js.Array[Node] | DataSet[Node]
  val edges: js.Array[Edge] | DataSet[Edge]
}

trait Node extends NodeOptions {
  val id: IdType
}

trait Edge extends EdgeOptions {
  val from: IdType
  val to: IdType
  val id: IdType
}

trait Image extends js.Object {
  val unselected: js.UndefOr[String] = js.undefined
  val selected: js.UndefOr[String] = js.undefined
}

trait ImagePadding extends js.Object {
  val top: js.UndefOr[Double] = js.undefined
  val right: js.UndefOr[Double] = js.undefined
  val bottom: js.UndefOr[Double] = js.undefined
  val left: js.UndefOr[Double] = js.undefined
}

/** See [[https://visjs.github.io/vis-network/docs/network/nodes.html]] */
trait NodeOptions extends js.Object {
  val borderWidth: js.UndefOr[Double] = js.undefined
  val borderWidthSelected: js.UndefOr[Double] = js.undefined
  val brokenImage: js.UndefOr[String] = js.undefined
  val color: js.UndefOr[String | NodeOptions.Color] = js.undefined
  val opacity: js.UndefOr[Double] = js.undefined
  val fixed: js.UndefOr[Boolean | NodeOptions.Fixed] = js.undefined
  val font: js.UndefOr[String | Font] = js.undefined
  val group: js.UndefOr[String] = js.undefined
  val hidden: js.UndefOr[Boolean] = js.undefined
  val icon: js.UndefOr[NodeOptions.Icon] = js.undefined
  val image: js.UndefOr[String | Image] = js.undefined
  val imagePadding: js.UndefOr[Double | ImagePadding] = js.undefined
  val label: js.UndefOr[String] = js.undefined
  val labelHighlightBold: js.UndefOr[Boolean] = js.undefined
  val level: js.UndefOr[Double] = js.undefined
  val margin: js.UndefOr[NodeOptions.Margin] = js.undefined
  val mass: js.UndefOr[Double] = js.undefined
  val physics: js.UndefOr[Boolean] = js.undefined
  val scaling: js.UndefOr[OptionsScaling] = js.undefined
  val shadow: js.UndefOr[Boolean | OptionsShadow] = js.undefined
  val shape: js.UndefOr[String] = js.undefined
  val shapeProperties: js.UndefOr[NodeOptions.ShapeProperties] = js.undefined
  val size: js.UndefOr[Double] = js.undefined
  val title: js.UndefOr[String] = js.undefined
  val value: js.UndefOr[Double] = js.undefined
  val widthConstraint: js.UndefOr[Double | Boolean | NodeOptions.WidthConstraint] = js.undefined
  val x: js.UndefOr[Double] = js.undefined
  val y: js.UndefOr[Double] = js.undefined
}

object NodeOptions {

  trait Color extends js.Object {
    val border: js.UndefOr[String] = js.undefined
    val background: js.UndefOr[String] = js.undefined
    val highlight: js.UndefOr[String | Color.Highlight] = js.undefined
    val hover: js.UndefOr[String | Color.Hover] = js.undefined
  }

  object Color {

    trait Highlight extends js.Object {
      val border: js.UndefOr[String] = js.undefined
      val background: js.UndefOr[String] = js.undefined
    }

    trait Hover extends js.Object {
      val border: js.UndefOr[String] = js.undefined
      val background: js.UndefOr[String] = js.undefined
    }
  }

  trait Fixed extends js.Object {
    val x: js.UndefOr[Double] = js.undefined
    val y: js.UndefOr[Double] = js.undefined
  }

  trait Icon extends js.Object {
    val face: js.UndefOr[String] = js.undefined
    val code: js.UndefOr[String] = js.undefined
    val size: js.UndefOr[Double] = js.undefined
    val color: js.UndefOr[String] = js.undefined
    val weight: js.UndefOr[Double | String] = js.undefined
  }

  trait Margin extends js.Object {
    val top: js.UndefOr[Double] = js.undefined
    val right: js.UndefOr[Double] = js.undefined
    val bottom: js.UndefOr[Double] = js.undefined
    val left: js.UndefOr[Double] = js.undefined
  }

  trait ShapeProperties extends js.Object {
    val borderDashes: js.UndefOr[Boolean | js.Array[Double]] = js.undefined
    val borderRadius: js.UndefOr[Double] = js.undefined
    val interpolation: js.UndefOr[Boolean] = js.undefined
    val useImageSize: js.UndefOr[Boolean] = js.undefined
    val useBorderWithImage: js.UndefOr[Boolean] = js.undefined
  }

  trait WidthConstraint extends js.Object {
    val minimum: js.UndefOr[Double] = js.undefined
    val maximum: js.UndefOr[Double] = js.undefined
  }
}

trait Font extends js.Object {
  val color: js.UndefOr[String] = js.undefined
  val size: js.UndefOr[Double] = js.undefined
  val face: js.UndefOr[String] = js.undefined
  val background: js.UndefOr[String] = js.undefined
  val strokeWidt: js.UndefOr[Double] = js.undefined
  val strokeColor: js.UndefOr[String] = js.undefined
  val align: js.UndefOr[String] = js.undefined
  val vadjust: js.UndefOr[Double] = js.undefined
  val multi: js.UndefOr[String] = js.undefined
  val bold: js.UndefOr[String | FontOptions] = js.undefined
  val ital: js.UndefOr[String | FontOptions] = js.undefined
  val boldital: js.UndefOr[String | FontOptions] = js.undefined
  val mono: js.UndefOr[String | FontOptions] = js.undefined
}

/** See [[https://visjs.github.io/vis-network/docs/network/edges.html]] */
trait EdgeOptions extends js.Object {
  val arrows: js.UndefOr[String | EdgeOptions.Arrows] = js.undefined
  val arrowStrikethrough: js.UndefOr[Boolean] = js.undefined
  val color: js.UndefOr[String | EdgeOptions.Color] = js.undefined
  val dashes: js.UndefOr[Boolean | js.Array[Double]] = js.undefined
  val font: js.UndefOr[String | Font] = js.undefined
  val hidden: js.UndefOr[Boolean] = js.undefined
  val hoverWidth: js.UndefOr[Double] = js.undefined
  val label: js.UndefOr[String] = js.undefined
  val labelHighlightBold: js.UndefOr[Boolean] = js.undefined
  val length: js.UndefOr[Double] = js.undefined
  val physics: js.UndefOr[Boolean] = js.undefined
  val scaling: js.UndefOr[OptionsScaling] = js.undefined
  val selectionWidth: js.UndefOr[Double] = js.undefined
  val selfReferenceSize: js.UndefOr[Double] = js.undefined
  val selfReference: js.UndefOr[EdgeOptions.SelfReference] = js.undefined
  val shadow: js.UndefOr[Boolean | OptionsShadow] = js.undefined
  val smooth: js.UndefOr[Boolean | EdgeOptions.Smooth] = js.undefined
  val title: js.UndefOr[String] = js.undefined
  val value: js.UndefOr[Double] = js.undefined
  val width: js.UndefOr[Double] = js.undefined
}

object EdgeOptions {

  trait Arrows extends js.Object {
    val to: js.UndefOr[Boolean | ArrowsFormat] = js.undefined
    val middle: js.UndefOr[Boolean | ArrowsFormat] = js.undefined
    val from: js.UndefOr[Boolean | ArrowsFormat] = js.undefined
  }

  trait ArrowsFormat extends js.Object {
    val enabled: js.UndefOr[Boolean] = js.undefined
    val scaleFactor: js.UndefOr[Double] = js.undefined
    val `type`: js.UndefOr[String] = js.undefined
  }

  trait Color extends js.Object {
    val color: js.UndefOr[String] = js.undefined
    val highlight: js.UndefOr[String] = js.undefined
    val hover: js.UndefOr[String] = js.undefined
    val inherit: js.UndefOr[Boolean | String] = js.undefined
    val opacity: js.UndefOr[Double] = js.undefined
  }

  object Color {

    trait Highlight extends js.Object {
      val border: js.UndefOr[String] = js.undefined
      val background: js.UndefOr[String] = js.undefined
    }

    trait Hover extends js.Object {
      val border: js.UndefOr[String] = js.undefined
      val background: js.UndefOr[String] = js.undefined
    }
  }

  trait SelfReference extends js.Object {
    val size: js.UndefOr[Double] = js.undefined
    val angle: js.UndefOr[Double] = js.undefined
    val renderBehindTheNode: js.UndefOr[Boolean] = js.undefined
  }

  trait Smooth extends js.Object {
    val enabled: Boolean
    val `type`: String
    val forceDirection: js.UndefOr[String | Boolean] = js.undefined
    val roundness: Double
  }
}

trait FontOptions extends js.Object {
  val color: js.UndefOr[String] = js.undefined
  val size: js.UndefOr[Double] = js.undefined
  val face: js.UndefOr[String] = js.undefined
  val mod: js.UndefOr[String] = js.undefined
  val vadjust: js.UndefOr[Double] = js.undefined
}

trait OptionsScaling extends js.Object {
  val min: js.UndefOr[Double] = js.undefined
  val max: js.UndefOr[Double] = js.undefined
  val label: js.UndefOr[Boolean | OptionsScaling.Label] = js.undefined

  /** Function arguments: `min`, `max`, `total`, `value` */
  val customScalingFunction: js.UndefOr[js.Function4[
    js.UndefOr[Double],
    js.UndefOr[Double],
    js.UndefOr[Double],
    js.UndefOr[Double],
    Double
  ]]
}

object OptionsScaling {
  trait Label extends js.Object {
    val enabled: js.UndefOr[Boolean] = js.undefined
    val min: js.UndefOr[Double] = js.undefined
    val max: js.UndefOr[Double] = js.undefined
    val maxVisible: js.UndefOr[Double] = js.undefined
    val drawThreshold: js.UndefOr[Double] = js.undefined
  }
}

trait OptionsShadow extends js.Object {
  val enabled: Boolean
  val color: String
  val size: Double
  val x: Double
  val y: Double
}

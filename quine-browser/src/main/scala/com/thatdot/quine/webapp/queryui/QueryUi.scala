package com.thatdot.quine.webapp.queryui

import scala.collection.mutable
import scala.concurrent._
import scala.scalajs.js
import scala.scalajs.js.Dynamic.{literal => jsObj}
import scala.util.{Failure, Random, Success}

import cats.data.Validated
import com.raquo.laminar.api.L._
import endpoints4s.Invalid
import io.circe.Json
import io.circe.Printer.{noSpaces, spaces2}
import org.scalajs.dom
import org.scalajs.dom.{document, window}
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.api.v2.QueryWebSocketProtocol.QueryInterpreter
import com.thatdot.quine.Util.escapeHtml
import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.webapp.components.{
  ContextMenu,
  ContextMenuItem,
  CypherResultsTable,
  Loader,
  VisData,
  VisIndirectMouseEvent,
  VisNetwork,
}
import com.thatdot.quine.webapp.queryui.{
  DownloadUtils,
  HistoryJsonSchema,
  HistoryNavigationButtons,
  MessageBar,
  MessageBarContent,
  NetworkLayout,
  QueryMethod,
  QueryUiEvent,
  QueryUiVisEdgeExt,
  QueryUiVisNodeExt,
  SvgSnapshot,
  TopBar,
  UiQueryType,
}
import com.thatdot.quine.webapp.{History, QueryUiOptions, Styles}
import com.thatdot.{visnetwork => vis}

object QueryUi {

  case class Props(
    routes: ClientRoutes,
    graphData: VisData,
    initialQuery: String = "",
    nodeResultSizeLimit: Long = 100,
    hostColors: Vector[String] = Vector("#97c2fc", "green", "purple", "blue", "red", "orange", "yellow", "black"),
    onNetworkCreate: Option[js.Function1[vis.Network, js.Any]] = None,
    isQueryBarVisible: Boolean = true,
    showEdgeLabels: Boolean = true,
    showHostInTooltip: Boolean = true,
    initialAtTime: Option[Long] = None,
    initialLayout: NetworkLayout = NetworkLayout.Graph,
    edgeQueryLanguage: QueryLanguage = QueryLanguage.Cypher,
    queryMethod: QueryMethod = QueryMethod.WebSocket,
    initialNamespace: NamespaceParameter = NamespaceParameter.defaultNamespaceParameter,
    permissions: Option[Set[String]] = None,
  )

  case class State(
    query: String,
    pendingTextQueries: Set[QueryId],
    queryBarColor: Option[String],
    sampleQueries: Vector[SampleQuery],
    history: History[QueryUiEvent],
    animating: Boolean,
    foundNodesCount: Option[Int],
    foundEdgesCount: Option[Int],
    runningQueryCount: Long,
    uiNodeQuickQueries: Vector[UiNodeQuickQuery],
    uiNodeAppearances: Vector[UiNodeAppearance],
    atTime: Option[Long],
    namespace: NamespaceParameter,
    areSampleQueriesVisible: Boolean,
  )

  private case class ContextMenuState(x: Double, y: Double, items: Seq[ContextMenuItem])

  def apply(props: Props): HtmlElement = {

    val stateVar = Var(
      State(
        query = props.initialQuery,
        pendingTextQueries = Set.empty,
        queryBarColor = None,
        sampleQueries = Vector.empty,
        history = History.empty,
        animating = false,
        foundNodesCount = None,
        foundEdgesCount = None,
        runningQueryCount = 0,
        uiNodeQuickQueries = UiNodeQuickQuery.defaults,
        uiNodeAppearances = Vector.empty,
        atTime = props.initialAtTime,
        namespace = props.initialNamespace,
        areSampleQueriesVisible = false,
      ),
    )

    // Separate Vars for state containing Laminar elements (to avoid remounting issues)
    val contextMenuVar = Var(Option.empty[ContextMenuState])
    val bottomBarVar = Var(Option.empty[MessageBarContent])

    // Mutable refs
    var network: Option[vis.Network] = None
    var layout: NetworkLayout = props.initialLayout
    val visualization: GraphVisualization = new VisNetworkVisualization(props.graphData, () => network)
    val pinTracker = new PinTracker(visualization)
    var webSocketClientFut: Future[WebSocketQueryClient] =
      Future.failed(new Exception("Client not initialized"))
    var webSocketClientV2Fut: Future[V2WebSocketQueryClient] =
      Future.failed(new Exception("V2 client not initialized"))

    def selectedInterpreter: QueryInterpreter = QueryInterpreter.Cypher

    // --- WebSocket client management ---

    def getWebSocketClient(): Future[WebSocketQueryClient] = {
      webSocketClientFut.value match {
        case Some(Success(client)) if client.webSocket.readyState == dom.WebSocket.OPEN => ()
        case None => ()
        case Some(_) =>
          val client = props.routes.queryProtocolClient()
          val clientReady = Promise[WebSocketQueryClient]()
          val webSocket = client.webSocket

          webSocket
            .addEventListener[dom.MessageEvent]("open", (_: dom.MessageEvent) => clientReady.trySuccess(client))
          webSocket.addEventListener[dom.Event](
            "error",
            (_: dom.Event) =>
              clientReady.tryFailure(new Exception(s"WebSocket connection to `${webSocket.url}` failed")),
          )
          webSocket.addEventListener[dom.CloseEvent](
            "close",
            (_: dom.CloseEvent) =>
              clientReady.tryFailure(new Exception(s"WebSocket connection to `${webSocket.url}` was closed")),
          )

          webSocketClientFut = clientReady.future
      }
      webSocketClientFut
    }

    /** Get a V2 websocket client
      *
      * Same reconnection logic as [[getWebSocketClient]] but creates a [[V2WebSocketQueryClient]]
      * connected to `/api/v2/query/ws`. Unlike V1, the V2 endpoint supports a namespace parameter.
      */
    def getWebSocketClientV2(): Future[V2WebSocketQueryClient] = {
      webSocketClientV2Fut.value match {
        case Some(Success(client)) if client.webSocket.readyState == dom.WebSocket.OPEN => ()
        case None => ()
        case Some(_) =>
          val ns = Option(stateVar.now().namespace.namespaceId).filterNot(_ == "default")
          val client = props.routes.queryProtocolClientV2(ns)
          val clientReady = Promise[V2WebSocketQueryClient]()
          val webSocket = client.webSocket

          webSocket.addEventListener[dom.Event]("open", (_: dom.Event) => clientReady.trySuccess(client))
          webSocket.addEventListener[dom.Event](
            "error",
            (_: dom.Event) =>
              clientReady.tryFailure(new Exception(s"WebSocket connection to `${webSocket.url}` failed")),
          )
          webSocket.addEventListener[dom.CloseEvent](
            "close",
            (_: dom.CloseEvent) =>
              clientReady.tryFailure(new Exception(s"WebSocket connection to `${webSocket.url}` was closed")),
          )

          webSocketClientV2Fut = clientReady.future
      }
      webSocketClientV2Fut
    }

    // --- Node appearance and quick queries ---

    def quickQueriesFor(node: UiNode[String]): Seq[QuickQuery] =
      stateVar.now().uiNodeQuickQueries.collect {
        case UiNodeQuickQuery(predicate, qq) if predicate.matches(node) => qq
      }

    def appearanceFor(node: UiNode[String]): (String, vis.NodeOptions.Icon) = {
      val (sizeOpt, iconOpt, colorOpt, labelDescOpt) = stateVar
        .now()
        .uiNodeAppearances
        .find(_.predicate.matches(node))
        .map(appearance => (appearance.size, appearance.icon, appearance.color, appearance.label))
        .getOrElse((None, None, None, None))

      val visIcon = new vis.NodeOptions.Icon {
        override val color =
          colorOpt.getOrElse[String](props.hostColors(Math.floorMod(node.hostIndex, props.hostColors.length)))
        override val code = iconOpt.getOrElse[String]("\uf3a6")
        override val size = sizeOpt.getOrElse[Double](30.0)
      }

      val uiLabel = labelDescOpt match {
        case Some(UiNodeLabel.Constant(lbl)) => lbl
        case Some(UiNodeLabel.Property(key, prefix)) if node.properties.contains(key) =>
          val prop = node.properties(key)
          val propVal = prop.asString getOrElse prop.noSpaces
          prefix.getOrElse("") + propVal
        case _ => node.label
      }

      uiLabel -> visIcon
    }

    // --- Conversion helpers ---

    def nodeUi2Vis(node: UiNode[String], startingPosition: Option[(Double, Double)]): vis.Node = {
      val (uiLabel, iconStyle) = appearanceFor(node)

      new QueryUiVisNodeExt {
        override val id = node.id
        override val label = uiLabel
        override val icon = iconStyle
        override val uiNode = node

        override val x = startingPosition match {
          case Some((xPos, _)) => xPos
          case None => js.undefined
        }
        override val y = startingPosition match {
          case Some((_, yPos)) => yPos
          case None => js.undefined
        }

        override val title = {
          val idProp = s"<strong>ID : ${escapeHtml(node.id)}</strong>"
          val hostProp = if (props.showHostInTooltip) {
            List(s"<strong>Served from Host : ${node.hostIndex}</strong>")
          } else {
            Nil
          }
          val strProps = node.properties.toList
            .sortBy(_._1)
            .map { case (keyStr, valueJson) =>
              s"${escapeHtml(keyStr)} : ${escapeHtml(valueJson.noSpaces)}"
            }

          (idProp :: hostProp ++ strProps).mkString("<br>")
        }
      }
    }

    def edgeId(edge: UiEdge[String]): String =
      s"${edge.from}-${edge.edgeType}->${edge.to}"

    def edgeUi2Vis(edge: UiEdge[String], isSynEdge: Boolean): vis.Edge = new QueryUiVisEdgeExt {
      override val id = edgeId(edge)
      override val from = edge.from
      override val to = edge.to
      override val label = if (props.showEdgeLabels) edge.edgeType else js.undefined
      override val arrows = if (edge.isDirected) "to" else ""
      override val smooth = isSynEdge
      override val uiEdge = edge
      override val isSyntheticEdge = isSynEdge
      override val color = if (isSynEdge) "purple" else js.undefined
      override val dashes = if (isSynEdge) true else js.undefined
    }

    // --- History event application ---

    implicit lazy val queryUiEvent: History.Event[QueryUiEvent] = new History.Event[QueryUiEvent] {
      import QueryUiEvent._
      import js.JSConverters._

      def applyEvent(event: QueryUiEvent): Unit = event match {
        case Add(nodes, edges, updateNodes, syntheticEdges, explodeFromId) =>
          window.setTimeout(() => animateNetwork(1000), 0)

          val posOpt: Option[(Double, Double)] = explodeFromId.map { startingId =>
            val bb = network.get.getBoundingBox(startingId)
            ((bb.left + bb.right) / 2, (bb.top + bb.bottom) / 2)
          }

          props.graphData.nodeSet.add(nodes.map(nodeUi2Vis(_, posOpt)).toJSArray)
          props.graphData.edgeSet.add(edges.map(edgeUi2Vis(_, isSynEdge = false)).toJSArray)
          props.graphData.edgeSet.add(syntheticEdges.map(edgeUi2Vis(_, isSynEdge = true)).toJSArray)
          props.graphData.nodeSet.update(updateNodes.map(nodeUi2Vis(_, None)).toJSArray)

          if (layout == NetworkLayout.Tree) {
            network.get.setOptions(new vis.Network.Options {
              override val nodes = new vis.NodeOptions {
                override val shape = "icon"
              }
            })
          }

        case Remove(nodes, edges, _, syntheticEdges, _) =>
          props.graphData.nodeSet.remove(nodes.map(n => n.id: vis.IdType).toJSArray)
          props.graphData.edgeSet.remove(edges.map(e => edgeId(e): vis.IdType).toJSArray)
          props.graphData.edgeSet.remove(syntheticEdges.map(e => edgeId(e): vis.IdType).toJSArray)
          pinTracker.removeNodes(nodes.map(_.id))
          ()

        case Collapse(nodes, clusterId, name) =>
          val nodeIds: Set[vis.IdType] = nodes.map(id => id: vis.IdType).toSet
          window.setTimeout(() => animateNetwork(1000), 0)
          network.get.cluster(new vis.ClusterOptions {
            override val joinCondition = Some[js.Function1[js.Any, Boolean]]((n: js.Any) =>
              nodeIds.contains(n.asInstanceOf[vis.Node].id),
            ).orUndefined

            override val processProperties = Some[js.Function3[js.Any, js.Any, js.Any, js.Any]] {
              (clusterOptionsAny: js.Any, childNodes: js.Any, childEdges: js.Any) =>
                trait MutableClusterOptions extends js.Object {
                  var id: js.UndefOr[vis.IdType]
                  var label: js.UndefOr[String]
                  var title: js.UndefOr[String]
                  var collapsedNodes: js.UndefOr[js.Array[String]]
                }

                val clusterOptions = clusterOptionsAny.asInstanceOf[MutableClusterOptions]
                clusterOptions.id = clusterId
                clusterOptions.label = name
                clusterOptions.collapsedNodes = nodes.toJSArray

                clusterOptions
            }.orUndefined

            override val clusterNodeProperties = new vis.NodeOptions {
              override val icon = new vis.NodeOptions.Icon {
                override val code = "\uf413"
                override val size = 54
              }
            }
          })

        case Expand(_, clusterId, _) =>
          window.setTimeout(() => animateNetwork(1000), 0)
          network.get.openCluster(clusterId)

        case Checkpoint(_) =>
        case Layout(positions) =>
          pinTracker.resetStateOnly(positions.collect { case (id, NodePosition(_, _, true)) => id }.toSet)
          for ((nodeId, NodePosition(x, y, isFixed)) <- positions) {
            visualization.setNodePosition(nodeId, x, y)
            if (isFixed) visualization.pinNode(nodeId) else visualization.unpinNode(nodeId)
          }
      }

      def invert(event: QueryUiEvent) = event.invert
    }

    // --- History management ---

    def updateHistory(
      update: History[QueryUiEvent] => Option[History[QueryUiEvent]],
      callback: () => Unit = () => (),
    ): Unit = {
      stateVar.update(s => update(s.history).fold(s)(h => s.copy(history = h)))
      callback()
    }

    def downloadHistoryFile(history: History[QueryUiEvent], fileName: String): Unit =
      DownloadUtils.downloadFile(HistoryJsonSchema.encode(history), fileName, "application/json")

    def uploadHistory(files: dom.FileList): Unit = {
      val file = if (files.length != 1) {
        val msg = s"Expected one file, but got ${files.length}"
        bottomBarVar.set(Some(MessageBarContent(pre(msg), Styles.queryResultError)))
        return
      } else {
        files(0)
      }

      if (file.`type` != "application/json") {
        val msg = s"Expected JSON file, but `${file.name}' has type '${file.`type`}'."
        bottomBarVar.set(Some(MessageBarContent(pre(msg), Styles.queryResultError)))
        return
      }

      val reader = new dom.FileReader()
      reader.onload = (e: dom.ProgressEvent) => {
        val jsonStr = e.target.asInstanceOf[dom.FileReader].result.asInstanceOf[String]
        HistoryJsonSchema.decode(jsonStr) match {
          case Validated.Valid(hist) =>
            val msg = """Uploading this history will erase your existing one.
                        |
                        |Do you wish to continue?""".stripMargin
            if (window.confirm(msg)) {
              stateVar.update(_.copy(history = hist))
              hist.past.reverse.foreach(queryUiEvent.applyEvent(_))
            }

          case Validated.Invalid(errs) =>
            val msg = s"Malformed JSON history file:${errs.toList.mkString("\n  ", "\n  ", "")}"
            bottomBarVar.set(Some(MessageBarContent(pre(msg), Styles.queryResultError)))
        }
      }
      reader.readAsText(file)
    }

    // --- SVG / download ---

    def downloadSvgSnapshot(fileName: String = "graph.svg"): Unit = {
      val positions = network.get.getPositions(props.graphData.nodeSet.getIds())
      SvgSnapshot(props.graphData, positions).map { svgElement =>
        val tempContainer = document.createElement("div")
        tempContainer.setAttribute("style", "position: absolute; visibility: hidden; pointer-events: none;")
        document.body.appendChild(tempContainer)
        tempContainer.appendChild(svgElement)

        val svgEl = svgElement.asInstanceOf[dom.svg.SVG]
        val bbox = svgEl.getBBox()
        val padding = 10
        val viewBoxMinX = bbox.x - padding
        val viewBoxMinY = bbox.y - padding
        val viewBoxWidth = bbox.width + 2 * padding
        val viewBoxHeight = bbox.height + 2 * padding
        svgEl.setAttribute("viewBox", s"$viewBoxMinX $viewBoxMinY $viewBoxWidth $viewBoxHeight")
        svgEl.setAttribute("width", s"${viewBoxWidth}px")
        svgEl.setAttribute("height", s"${viewBoxHeight}px")

        val blob = new dom.Blob(
          js.Array(tempContainer.innerHTML),
          new dom.BlobPropertyBag { `type` = "image/svg" },
        )

        document.body.removeChild(tempContainer)

        val a = document.createElement("a").asInstanceOf[dom.HTMLAnchorElement]
        a.setAttribute("download", fileName)
        a.setAttribute("href", dom.URL.createObjectURL(blob))
        a.setAttribute("target", "_blank")
        a.click()
      }
      ()
    }

    def makeSnapshot(): History[QueryUiEvent] = {
      val nodes: Seq[UiNode[String]] = props.graphData.nodeSet
        .get()
        .toSeq
        .map(_.asInstanceOf[QueryUiVisNodeExt].uiNode)

      val (syntheticEdges, edges) = props.graphData.edgeSet
        .get()
        .toSeq
        .map(_.asInstanceOf[QueryUiVisEdgeExt])
        .partition(_.isSyntheticEdge)

      History(
        past = List(QueryUiEvent.Add(nodes, edges.map(_.uiEdge), Seq.empty, syntheticEdges.map(_.uiEdge), None)),
        future = List(),
      )
    }

    def downloadGraphJsonLd(): Unit =
      networkLayout { () =>
        DownloadUtils.downloadGraphJsonLd(props.graphData.nodeSet, props.graphData.edgeSet)
      }

    // --- Query logic ---

    lazy val cypherQueryRegex = js.RegExp(
      raw"^\s*(optional|match|return|unwind|create|foreach|merge|call|load|with|explain|show|profile)[^a-z]",
      flags = "i",
    )

    def guessQueryLanguage(query: String): QueryLanguage = cypherQueryRegex.test(query) match {
      case true => QueryLanguage.Cypher
      case false => QueryLanguage.Gremlin
    }

    def invalidToException(invalid: Invalid): Exception = new Exception(invalid.errors mkString "\n")

    def mergeEndpointErrorsIntoFuture[A](fut: Future[Either[endpoints4s.Invalid, Option[A]]]): Future[A] =
      fut.flatMap { either =>
        Future.fromTry {
          either.left
            .map(invalidToException)
            .flatMap(_.toRight(new NoSuchElementException()))
            .toTry
        }
      }

    def nodeQuery(
      query: String,
      namespace: NamespaceParameter,
      atTime: Option[Long],
      language: QueryLanguage,
      parameters: Map[String, Json],
    ): Future[Option[Seq[UiNode[String]]]] =
      props.queryMethod match {
        case QueryMethod.Restful =>
          mergeEndpointErrorsIntoFuture(language match {
            case QueryLanguage.Gremlin =>
              props.routes.gremlinNodesPost((atTime, None, namespace, GremlinQuery(query))).future
            case QueryLanguage.Cypher =>
              props.routes.cypherNodesPost((atTime, None, namespace, CypherQuery(query))).future
          }).map(Some(_))

        case QueryMethod.RestfulV2 =>
          mergeEndpointErrorsIntoFuture(language match {
            case QueryLanguage.Gremlin =>
              Future.successful(Left(Invalid(Seq("Gremlin is not supported in APIv2"))))
            case QueryLanguage.Cypher =>
              props.routes.cypherNodesPostV2((atTime, None, namespace, CypherQuery(query))).future
          }).map(Some(_))

        case QueryMethod.WebSocket =>
          val nodeCallback = new QueryCallbacks.CollectNodesToFuture()
          val streamingQuery = StreamingQuery(query, parameters, language, atTime, None, Some(100))
          for {
            client <- getWebSocketClient()
            _ <- Future.fromTry(client.query(streamingQuery, nodeCallback).toTry)
            results <- nodeCallback.future
          } yield results

        case QueryMethod.WebSocketV2 =>
          val cb = new V2QueryCallbacks.CollectNodesToFuture()
          val interpreter = selectedInterpreter
          val sq =
            V2StreamingQuery(query, parameters, interpreter = interpreter, atTime = atTime, maxResultBatch = Some(100))
          for {
            client <- getWebSocketClientV2()
            _ <- Future.fromTry(client.query(sq, cb).toTry)
            results <- cb.future
          } yield results.map(_.map(n => UiNode(n.id, n.hostIndex, n.label, n.properties)))
      }

    def edgeQuery(
      query: String,
      atTime: Option[Long],
      namespace: NamespaceParameter,
      language: QueryLanguage,
      parameters: Map[String, Json],
    ): Future[Option[Seq[UiEdge[String]]]] =
      props.queryMethod match {
        case QueryMethod.Restful =>
          mergeEndpointErrorsIntoFuture(language match {
            case QueryLanguage.Gremlin =>
              props.routes.gremlinEdgesPost((atTime, None, namespace, GremlinQuery(query, parameters))).future
            case QueryLanguage.Cypher =>
              props.routes.cypherEdgesPost((atTime, None, namespace, CypherQuery(query, parameters))).future
          }).map(Some(_))

        case QueryMethod.RestfulV2 =>
          mergeEndpointErrorsIntoFuture(language match {
            case QueryLanguage.Gremlin =>
              Future.successful(Left(Invalid(Seq("Gremlin is not supported in APIv2"))))
            case QueryLanguage.Cypher =>
              props.routes.cypherEdgesPostV2((atTime, None, namespace, CypherQuery(query, parameters))).future
          }).map(Some(_))

        case QueryMethod.WebSocket =>
          val edgeCallback = new QueryCallbacks.CollectEdgesToFuture()
          val streamingQuery = StreamingQuery(query, parameters, language, atTime, None, Some(100))
          for {
            client <- getWebSocketClient()
            _ <- Future.fromTry(client.query(streamingQuery, edgeCallback).toTry)
            results <- edgeCallback.future
          } yield results

        case QueryMethod.WebSocketV2 =>
          val cb = new V2QueryCallbacks.CollectEdgesToFuture()
          val interpreter = selectedInterpreter
          val sq =
            V2StreamingQuery(query, parameters, interpreter = interpreter, atTime = atTime, maxResultBatch = Some(100))
          for {
            client <- getWebSocketClientV2()
            _ <- Future.fromTry(client.query(sq, cb).toTry)
            results <- cb.future
          } yield results.map(_.map(e => UiEdge(e.from, e.edgeType, e.to, e.isDirected)))
      }

    def textQuery(
      query: String,
      atTime: Option[Long],
      namespace: NamespaceParameter,
      language: QueryLanguage,
      parameters: Map[String, Json],
      updateResults: Either[Seq[Json], CypherQueryResult] => Unit,
    ): Future[Option[Unit]] =
      (props.queryMethod, language) match {
        case (QueryMethod.Restful, QueryLanguage.Gremlin) =>
          val gremlinResults =
            props.routes.gremlinPost((atTime, None, namespace, GremlinQuery(query, parameters))).future
          mergeEndpointErrorsIntoFuture(gremlinResults).map { results =>
            updateResults(Left(results))
            Some(())
          }

        case (QueryMethod.Restful, QueryLanguage.Cypher) =>
          val cypherResults =
            props.routes.cypherPost((atTime, None, namespace, CypherQuery(query, parameters))).future
          mergeEndpointErrorsIntoFuture(cypherResults).map { results =>
            updateResults(Right(results))
            Some(())
          }

        case (QueryMethod.RestfulV2, QueryLanguage.Gremlin) =>
          Future.successful(Some(()))

        case (QueryMethod.RestfulV2, QueryLanguage.Cypher) =>
          val cypherResults =
            props.routes.cypherPostV2((atTime, None, namespace, CypherQuery(query, parameters))).future
          mergeEndpointErrorsIntoFuture(cypherResults).map { results =>
            updateResults(Right(results))
            Some(())
          }

        case (QueryMethod.WebSocket, _) =>
          val result = Promise[Option[Unit]]()

          val textCallback: QueryCallbacks = language match {
            case QueryLanguage.Gremlin =>
              new QueryCallbacks.NonTabularCallbacks {
                private var buffered = Seq.empty[Json]
                private var cancelled = false

                def onNonTabularResults(batch: Seq[Json]): Unit = {
                  buffered ++= batch
                  updateResults(Left(buffered))
                }
                def onError(message: String): Unit = {
                  result.tryFailure(new Exception(message))
                  ()
                }
                def onComplete(): Unit = {
                  result.trySuccess(if (cancelled) None else Some(()))
                  ()
                }

                def onQueryStart(
                  isReadOnly: Boolean,
                  canContainAllNodeScan: Boolean,
                  columns: Option[Seq[String]],
                ): Unit = ()

                def onQueryCancelOk(): Unit = cancelled = true
                def onQueryCancelError(message: String): Unit = ()
              }

            case QueryLanguage.Cypher =>
              new QueryCallbacks.TabularCallbacks {
                private var buffered = Seq.empty[Seq[Json]]
                private var cancelled = false

                def onTabularResults(columns: Seq[String], batch: Seq[Seq[Json]]): Unit = {
                  buffered ++= batch
                  updateResults(Right(CypherQueryResult(columns, buffered)))
                }
                def onError(message: String): Unit = {
                  result.tryFailure(new Exception(message))
                  ()
                }
                def onComplete(): Unit = {
                  result.trySuccess(if (cancelled) None else Some(()))
                  ()
                }

                def onQueryStart(
                  isReadOnly: Boolean,
                  canContainAllNodeScan: Boolean,
                  columns: Option[Seq[String]],
                ): Unit =
                  for (cols <- columns)
                    updateResults(Right(CypherQueryResult(cols, buffered)))

                def onQueryCancelOk(): Unit = cancelled = true
                def onQueryCancelError(message: String): Unit = ()
              }
          }
          val streamingQuery = StreamingQuery(query, parameters, language, atTime, Some(1000), Some(100))
          for {
            client <- getWebSocketClient()
            queryId <- Future.fromTry(client.query(streamingQuery, textCallback).toTry)
            _ = {
              stateVar.update(s => s.copy(pendingTextQueries = s.pendingTextQueries + queryId))
              result.future.onComplete { _ =>
                stateVar.update(s => s.copy(pendingTextQueries = s.pendingTextQueries - queryId))
              }
            }
            results <- result.future
          } yield results

        case (QueryMethod.WebSocketV2, _) =>
          val result = Promise[Option[Unit]]()

          val textCallback = new V2QueryCallbacks.TextCallbacks {
            private var buffered = Seq.empty[Seq[Json]]
            private var cancelled = false

            override def onTabularResults(columns: Seq[String], batch: Seq[Seq[Json]]): Unit = {
              buffered ++= batch
              updateResults(Right(CypherQueryResult(columns, buffered)))
            }
            def onError(message: String): Unit = {
              result.tryFailure(new Exception(message))
              ()
            }
            def onComplete(): Unit = {
              result.trySuccess(if (cancelled) None else Some(()))
              ()
            }

            def onQueryStart(
              isReadOnly: Boolean,
              canContainAllNodeScan: Boolean,
              columns: Option[Seq[String]],
            ): Unit =
              for (cols <- columns)
                updateResults(Right(CypherQueryResult(cols, buffered)))

            def onQueryCancelOk(): Unit = cancelled = true
            def onQueryCancelError(message: String): Unit = ()
          }
          val interpreter = selectedInterpreter
          val sq = V2StreamingQuery(
            query,
            parameters,
            interpreter = interpreter,
            atTime = atTime,
            maxResultBatch = Some(1000),
            resultsWithinMillis = Some(100),
          )
          for {
            client <- getWebSocketClientV2()
            queryId <- Future.fromTry(client.query(sq, textCallback).toTry)
            _ = {
              stateVar.update(s => s.copy(pendingTextQueries = s.pendingTextQueries + queryId))
              result.future.onComplete { _ =>
                stateVar.update(s => s.copy(pendingTextQueries = s.pendingTextQueries - queryId))
              }
            }
            results <- result.future
          } yield results
      }

    lazy val ObserveStandingQuery = "(?:OBSERVE|observe) [\"']?(.*)[\"']?".r

    def submitQuery(uiQueryType: UiQueryType): Unit = {
      val state = stateVar.now()

      val query = state.query match {
        case ObserveStandingQuery(sqName) =>
          val amendedQuery = s"CALL standing.wiretap({ name: '$sqName' })"
          stateVar.update(_.copy(query = amendedQuery))
          amendedQuery
        case other =>
          other
      }
      val language = guessQueryLanguage(query)

      if (state.pendingTextQueries.nonEmpty) {
        window.alert(
          """You have a pending text query. You must cancel it before issuing another query.
            |Pending queries can be cancelled by clicking on the spinning loader in the top right.
            |""".stripMargin,
        )
        return
      }

      stateVar.update(s =>
        s.copy(
          foundNodesCount = None,
          foundEdgesCount = None,
          queryBarColor = None,
          runningQueryCount = s.runningQueryCount + 1,
        ),
      )
      bottomBarVar.set(None)

      if (uiQueryType == UiQueryType.Text) {

        def updateResults(result: Either[Seq[Json], CypherQueryResult]): Unit = {
          val rendered: HtmlElement = result match {
            case Left(results) =>
              val json = Json.fromValues(results)
              val indent = json.isObject || json.asArray.exists(_.exists(_.isObject))
              pre(if (indent) spaces2.print(json) else noSpaces.print(json))
            case Right(results) => CypherResultsTable(results)
          }
          bottomBarVar.set(Some(MessageBarContent(rendered, Styles.queryResultSuccess)))
        }

        textQuery(query, state.atTime, state.namespace, language, Map.empty, updateResults).onComplete {
          case Success(outcome) =>
            val outcomeColor = if (outcome.isEmpty) Styles.queryResultEmpty else Styles.queryResultSuccess
            stateVar.update(s =>
              s.copy(
                queryBarColor = Some(outcomeColor),
                runningQueryCount = s.runningQueryCount - 1,
              ),
            )
            bottomBarVar.update(_.map {
              case MessageBarContent(res, Styles.queryResultSuccess) => MessageBarContent(res, outcomeColor)
              case other => other
            })
            window.setTimeout(
              () => stateVar.update(_.copy(queryBarColor = None)),
              750,
            )
            ()

          case Failure(err) =>
            val failureBar = MessageBarContent(pre(err.getMessage), Styles.queryResultError)
            stateVar.update(s =>
              s.copy(
                queryBarColor = Some(Styles.queryResultError),
                runningQueryCount = s.runningQueryCount - 1,
              ),
            )
            bottomBarVar.set(Some(failureBar))
        }

      } else {

        val nodesEdgesFut = for {
          rawNodesOpt <- nodeQuery(query, state.namespace, state.atTime, language, Map.empty)
          dedupedNodesOpt = rawNodesOpt.map { rawNodes =>
            val dedupedIds = mutable.Set.empty[String]
            rawNodes.filter(n => dedupedIds.add(n.id))
          }

          nodes = dedupedNodesOpt match {
            case Some(dedupedNodes) if dedupedNodes.length > props.nodeResultSizeLimit =>
              val limitedCount: Option[Int] = Option(
                window.prompt(
                  s"You are about to render ${dedupedNodes.length} nodes.\nHow many do you want to render?",
                  dedupedNodes.length.toString,
                ),
              ).map(_.toInt)
              stateVar.update(s => s.copy(foundNodesCount = limitedCount))
              limitedCount.fold(Seq.empty[UiNode[String]])(dedupedNodes.take(_))

            case Some(dedupedNodes) =>
              stateVar.update(s => s.copy(foundNodesCount = Some(dedupedNodes.length)))
              dedupedNodes

            case None =>
              Nil
          }

          newNodes = nodes.map(_.id).toSet
          edgesOpt <-
            if (newNodes.isEmpty) {
              Future.successful(Some(Nil))
            } else {
              val edgeQueryStr = props.edgeQueryLanguage match {
                case QueryLanguage.Gremlin =>
                  """g.V(new).bothE().dedup().where(_.and(
                    | _.outV().strId().is(within(all)),
                    | _.inV().strId().is(within(all))))""".stripMargin

                case QueryLanguage.Cypher =>
                  """UNWIND $new AS newId
                    |CALL getFilteredEdges(newId, [], [], $all) YIELD edge
                    |RETURN DISTINCT edge AS e""".stripMargin
              }
              val existingNodes = props.graphData.nodeSet.getIds().map(_.toString).toVector
              val queryParameters = Map(
                "new" -> Json.fromValues(newNodes.map(Json.fromString)),
                "all" -> Json.fromValues((existingNodes ++ newNodes).map(Json.fromString)),
              )
              edgeQuery(edgeQueryStr, state.atTime, state.namespace, props.edgeQueryLanguage, queryParameters)
            }
          edges = edgesOpt match {
            case Some(edges) =>
              if (rawNodesOpt.nonEmpty) stateVar.update(s => s.copy(foundEdgesCount = Some(edges.length)))
              edges
            case None =>
              Nil
          }
        } yield (nodes, edges)

        nodesEdgesFut.onComplete {
          case Success((nodes, edges)) =>
            val (syntheticEdges, explodeFromIdOpt) = uiQueryType match {
              case UiQueryType.NodeFromId(explodeFromId, Some(syntheticEdgeLabel)) =>
                nodes.map(n => UiEdge(explodeFromId, syntheticEdgeLabel, n.id)) -> Some(explodeFromId)
              case UiQueryType.NodeFromId(explodeFromId, None) =>
                (Seq.empty, Some(explodeFromId))
              case UiQueryType.Node | UiQueryType.Text =>
                (Seq.empty, None)
            }

            val (newNodes, existingNodes) = nodes.partition(n => props.graphData.nodeSet.get(n.id) == null)
            val nodesToUpdate = existingNodes.filter { (node: UiNode[String]) =>
              val currentNode: vis.Node = props.graphData.nodeSet.get(node.id).merge
              val currentUiNode = currentNode.asInstanceOf[QueryUiVisNodeExt].uiNode
              node != currentUiNode
            }

            val addEvent = QueryUiEvent.Add(
              newNodes,
              edges.filter(e => props.graphData.edgeSet.get(edgeId(e)) == null),
              nodesToUpdate,
              syntheticEdges.filter(e => props.graphData.edgeSet.get(edgeId(e)) == null),
              explodeFromIdOpt,
            )

            if (addEvent.nonEmpty) {
              updateHistory(hist => Some(hist.observe(addEvent)))
            }
            stateVar.update(s => s.copy(runningQueryCount = s.runningQueryCount - 1))

          case Failure(err) =>
            val message = err.getMessage
            val contents = Seq.newBuilder[HtmlElement]
            contents += pre(cls := "wrap", if (message.isEmpty) "Cannot connect to server" else message)
            if (message.startsWith("TypeMismatchError Expected type(s) Node but got value")) {
              val failedQuery = stateVar.now().query
              contents += button(
                cls := "btn btn-link",
                onClick --> { _ =>
                  stateVar.update(_.copy(query = failedQuery))
                  submitQuery(UiQueryType.Text)
                },
                "Run again as text query",
              )
            }
            stateVar.update(s =>
              s.copy(
                queryBarColor = Some(Styles.queryResultError),
                runningQueryCount = s.runningQueryCount - 1,
              ),
            )
            bottomBarVar.set(
              Some(
                MessageBarContent(
                  div(contents.result()),
                  Styles.queryResultError,
                ),
              ),
            )
        }
      }
    }

    // --- Network management ---

    lazy val networkOptions = new vis.Network.Options {
      override val interaction = new vis.Network.Options.Interaction {
        override val hover = true
        override val tooltipDelay = 700
        override val zoomSpeed = 0.3
      }
      override val layout = new vis.Network.Options.Layout {
        override val hierarchical = false: js.Any
        override val improvedLayout = true
        override val randomSeed = 10203040
      }
      override val physics = new vis.Network.Options.Physics {
        override val forceAtlas2Based = new vis.Network.Options.Physics.ForceAtlas2Based {
          override val gravitationalConstant = -26
          override val centralGravity = 0.005
          override val springLength = 230
          override val springConstant = 0.18
          override val avoidOverlap = 1.5
        }
        override val maxVelocity = 25
        override val solver = "forceAtlas2Based"
        override val timestep = 0.25
        override val stabilization = new vis.Network.Options.Physics.Stabilization {
          override val enabled = true
          override val iterations = 150
          override val updateInterval = 25
        }
      }
      override val nodes = new vis.NodeOptions {
        override val shape = "icon"
        override val icon = new vis.NodeOptions.Icon {
          override val face = "Ionicons"
        }
      }
      override val edges = new vis.EdgeOptions {
        override val smooth = false
        override val arrows = "to"
      }
    }

    lazy val toggleNetworkLayout: () => Unit = { () =>
      layout match {
        case NetworkLayout.Graph =>
          layout = NetworkLayout.Tree
          network.get.setOptions(
            new vis.Network.Options {
              override val layout = new vis.Network.Options.Layout {
                override val hierarchical = jsObj(
                  enabled = true,
                  sortMethod = "directed",
                  shakeTowards = "roots",
                )
              }
            },
          )

        case NetworkLayout.Tree =>
          layout = NetworkLayout.Graph
          network.get.setOptions(
            new vis.Network.Options {
              override val layout = new vis.Network.Options.Layout {
                override val hierarchical = false: js.Any
              }
            },
          )

          network.get.setOptions(new vis.Network.Options {
            override val edges = new vis.EdgeOptions {
              override val smooth = false
              override val arrows = "to"
            }
          })
      }
      animateNetwork()
    }

    lazy val recenterNetworkViewport: () => Unit =
      () =>
        network.get.moveTo(
          new vis.MoveToOptions {
            override val position = new vis.Position {
              val x = 0d
              val y = 0d
            }
            override val scale = 1d
            override val animation = new vis.AnimationOptions {
              val duration = 2000d
              val easingFunction = "easeInOutCubic"
            }
          },
        )

    def animateNetwork(millis: Double = 1000): Unit = {
      stateVar.update(_.copy(animating = true))
      window.setTimeout(() => stateVar.update(_.copy(animating = false)), millis)
      ()
    }

    /** Unfix pinned nodes at drag start so vis-network allows repositioning.
      * Skipped when shift is held, since shift+click means "unpin", not "drag".
      */
    def networkDragStart(event: vis.ClickEvent): Unit = {
      if (event.event.asInstanceOf[VisIndirectMouseEvent].srcEvent.shiftKey) return
      val draggedIds = event.nodes.toSeq
        .map(_.asInstanceOf[String])
        .filterNot(nodeId => network.exists(_.isCluster(nodeId)))
      for (nodeId <- draggedIds if pinTracker.isPinned(nodeId))
        visualization.unfixForDrag(nodeId)
    }

    def networkDragEnd(event: vis.ClickEvent): Unit = {
      val draggedIds = event.nodes.toSeq
        .map(_.asInstanceOf[String])
        .filterNot(nodeId => network.exists(_.isCluster(nodeId)))
      if (draggedIds.nonEmpty)
        processVisualizationEvent(GraphVisualizationEvent.NodesMoved(draggedIds))
    }

    def networkClick(event: vis.ClickEvent): Unit =
      if (event.event.asInstanceOf[VisIndirectMouseEvent].srcEvent.shiftKey) {
        network.get.getNodeAt(event.pointer.DOM).toOption.foreach { nodeId =>
          val selected = network.get.getSelectedNodes()
          val ids = (if (selected.contains(nodeId)) selected.toSeq else Seq(nodeId))
            .map(_.asInstanceOf[String])
            .filterNot(id => network.exists(_.isCluster(id)))
          processVisualizationEvent(GraphVisualizationEvent.UnpinRequested(ids))
        }
      }

    def processVisualizationEvent(event: GraphVisualizationEvent): Unit = event match {
      case GraphVisualizationEvent.NodesMoved(nodeIds) =>
        pinTracker.pin(nodeIds)

      case GraphVisualizationEvent.UnpinRequested(nodeIds) =>
        pinTracker.unpinWithFlash(nodeIds)
    }

    def networkDeselect(event: vis.DeselectEvent): Unit = {
      if (!event.event.asInstanceOf[VisIndirectMouseEvent].srcEvent.shiftKey)
        return

      val clickedId = network.get.getNodeAt(event.pointer.DOM).toOption match {
        case None => return
        case Some(nodeId) => nodeId
      }

      val selection = event.previousSelection.nodes
      if (event.previousSelection.nodes.contains(clickedId)) {
        network.get.selectNodes(selection.filter(_ != clickedId))
      } else {
        network.get.selectNodes(selection :+ clickedId)
      }
    }

    def getContextMenuItems(nodeId: String, selectedIds: Seq[String]): Seq[ContextMenuItem] = {
      val contextMenuItems = Seq.newBuilder[ContextMenuItem]

      if (network.get.isCluster(nodeId)) {
        val expandOpt = stateVar
          .now()
          .history
          .past
          .find {
            case QueryUiEvent.Collapse(_, cId, _) => cId == nodeId
            case _ => false
          }
          .map(queryUiEvent.invert)

        contextMenuItems ++= expandOpt.toList.map { expand =>
          ContextMenuItem(
            item = "Expand cluster",
            title = "Expand cluster back into nodes",
            action = () => {
              contextMenuVar.set(None)
              updateHistory(hist => Some(hist.observe(expand)))
            },
          )
        }
      } else {
        val startingNodes: Seq[String] = if (selectedIds.contains(nodeId)) selectedIds else Seq(nodeId)

        val visNode: vis.Node = props.graphData.nodeSet.get(nodeId).merge
        val uiNode: UiNode[String] = visNode.asInstanceOf[QueryUiVisNodeExt].uiNode
        val quickQueries = if (startingNodes.size == 1) {
          quickQueriesFor(uiNode)
        } else {
          val intersectionOfPossibleQuickQueries = startingNodes.iterator
            .map { (startNodeId: String) =>
              val startingVisNode: vis.Node = props.graphData.nodeSet.get(startNodeId).merge
              val startingUiNode: UiNode[String] = startingVisNode.asInstanceOf[QueryUiVisNodeExt].uiNode
              quickQueriesFor(startingUiNode).filter(_.edgeLabel.isEmpty).toSet
            }
            .reduce(_ intersect _)
          quickQueriesFor(uiNode).filter(intersectionOfPossibleQuickQueries contains _)
        }

        contextMenuItems ++= quickQueries
          .map { qq: QuickQuery =>
            val queryType = qq.sort match {
              case QuerySort.Text => UiQueryType.Text
              case QuerySort.Node if startingNodes.size == 1 => UiQueryType.NodeFromId(uiNode.id, qq.edgeLabel)
              case QuerySort.Node => UiQueryType.Node
            }
            ContextMenuItem(
              item = qq.name,
              title = qq.querySuffix,
              action = () => {
                stateVar.update(_.copy(query = qq.fullQuery(startingNodes)))
                contextMenuVar.set(None)
                submitQuery(queryType)
              },
            )
          }
      }

      if (selectedIds.length > 1) {
        contextMenuItems += ContextMenuItem(
          item = "Collapse selected nodes",
          title = "Create a cluster node from selected nodes",
          action = () => {
            contextMenuVar.set(None)
            Option(window.prompt("Name your cluster:")).foreach { name =>
              updateHistory { hist =>
                val clusterId = "CLUSTER-" + Random.nextInt().toString.take(10)
                val clusterEvent = QueryUiEvent.Collapse(selectedIds, clusterId, name)
                Some(hist.observe(clusterEvent))
              }
            }
          },
        )
      }

      contextMenuItems.result()
    }

    def networkRightClick(event: vis.ClickEvent): Unit = {
      val contextMenuItems = network.get.getNodeAt(event.pointer.DOM).toOption match {
        case None =>
          Seq(
            ContextMenuItem(
              item = "Export SVG",
              title = "Download the current graph as an SVG image",
              action = () => {
                contextMenuVar.set(None)
                downloadSvgSnapshot()
              },
            ),
          )
        case Some(nodeId) =>
          getContextMenuItems(
            nodeId.asInstanceOf[String],
            event.nodes.toSeq.asInstanceOf[Seq[String]],
          )
      }

      contextMenuVar.set(
        Some(
          ContextMenuState(
            x = event.pointer.DOM.x,
            y = event.pointer.DOM.y,
            items = contextMenuItems,
          ),
        ),
      )
    }

    def networkDoubleClick(event: vis.ClickEvent): Unit = {
      val clickedId: String = if (event.nodes.length == 1) {
        event.nodes(0).asInstanceOf[String]
      } else {
        return
      }

      val contextMenuItems = getContextMenuItems(clickedId, Seq.empty)
      contextMenuItems.headOption.foreach(_.action())
    }

    def networkKeyDown(event: dom.KeyboardEvent): Unit =
      if (event.key == "Delete" || event.key == "Backspace") {
        event.preventDefault()
        val selectedIds = network.get.getSelectedNodes()
        if (selectedIds.nonEmpty) {
          val nodes: Seq[QueryUiEvent.Node] = selectedIds.map { (id: vis.IdType) =>
            val visNode: vis.Node = props.graphData.nodeSet.get(id).merge
            visNode.asInstanceOf[QueryUiVisNodeExt].uiNode
          }.toSeq
          val removeEvent = QueryUiEvent.Remove(nodes, Seq.empty, Seq.empty, Seq.empty, None)
          updateHistory(hist => Some(hist.observe(removeEvent)))
        }
      } else if (event.key == "a" && event.ctrlKey) {
        network.get.selectNodes(props.graphData.nodeSet.getIds())
      }

    def networkLayout(callback: () => Unit): Unit = {
      val coords = visualization.readNodePositions()
      val positions = coords.map { case (nodeId, (x, y)) =>
        nodeId -> QueryUiEvent.NodePosition(x, y, pinTracker.isPinned(nodeId))
      }
      val layoutEvent = QueryUiEvent.Layout(positions)
      updateHistory(hist => Some(hist.observe(layoutEvent)), callback)
    }

    def afterNetworkInit(net: vis.Network): Unit = {
      import vis._

      network = Some(net)
      props.onNetworkCreate.foreach(func => func(net))

      net.onDoubleClick(networkDoubleClick)
      net.onContext(networkRightClick)
      net.onDeselectNode(networkDeselect)
      net.onDragStart(networkDragStart)
      net.onDragEnd(networkDragEnd)
      net.onClick(networkClick)

      if (props.initialLayout != NetworkLayout.Graph) toggleNetworkLayout()
    }

    // --- Checkpoint navigation ---

    def stepBackToCheckpoint(name: Option[String] = None): Unit =
      stateVar.now().history.past match {
        case Nil =>
        case QueryUiEvent.Checkpoint(n) :: _ if name.forall(_ == n) =>
        case _ => updateHistory(_.stepBack(), () => stepBackToCheckpoint(name))
      }

    def stepForwardToCheckpoint(name: Option[String] = None): Unit =
      stateVar.now().history.future match {
        case Nil =>
        case QueryUiEvent.Checkpoint(n) :: _ if name.forall(_ == n) => updateHistory(_.stepForward())
        case _ => updateHistory(_.stepForward(), () => stepForwardToCheckpoint(name))
      }

    def checkpointContextMenuItems(): Seq[ContextMenuItem] = {
      val state = stateVar.now()
      val contextItems = Seq.newBuilder[ContextMenuItem]

      for (event <- state.history.future.reverse)
        event match {
          case QueryUiEvent.Checkpoint(name) =>
            val stepForward =
              () => {
                contextMenuVar.set(None)
                stepForwardToCheckpoint(Some(name))
              }
            contextItems += ContextMenuItem(div(name, em(" (future)")), name, stepForward)

          case _ =>
        }

      for ((event, idx) <- state.history.past.zipWithIndex)
        event match {
          case QueryUiEvent.Checkpoint(name) =>
            val item: HtmlElement = div(name, em(if (idx == 0) " (present)" else " (past)"))
            val stepBack =
              () => {
                contextMenuVar.set(None)
                stepBackToCheckpoint(Some(name))
              }
            contextItems += ContextMenuItem(item, name, stepBack)

          case _ =>
        }

      contextItems.result()
    }

    def setAtTime(atTime: Option[Long]): Unit =
      if (atTime != stateVar.now().atTime) {
        props.graphData.nodeSet.remove(props.graphData.nodeSet.getIds())
        props.graphData.edgeSet.remove(props.graphData.edgeSet.getIds())
        stateVar.update(
          _.copy(
            queryBarColor = None,
            history = History.empty,
            animating = false,
            foundNodesCount = None,
            foundEdgesCount = None,
            atTime = atTime,
          ),
        )
        bottomBarVar.set(None)
        contextMenuVar.set(None)
      }

    def cancelQueries(queryIds: Option[Iterable[QueryId]] = None): Unit =
      props.queryMethod match {
        case QueryMethod.WebSocket =>
          getWebSocketClient().value.flatMap(_.toOption).foreach { (client: WebSocketQueryClient) =>
            val queries = queryIds.getOrElse(client.activeQueries.keys).toList
            if (queries.nonEmpty && window.confirm(s"Cancel ${queries.size} running query execution(s)?")) {
              queries.foreach(queryId => client.cancelQuery(queryId))
            }
          }

        case QueryMethod.WebSocketV2 =>
          getWebSocketClientV2().value.flatMap(_.toOption).foreach { (client: V2WebSocketQueryClient) =>
            val queries = queryIds.getOrElse(client.activeQueries.keys).toList
            if (queries.nonEmpty && window.confirm(s"Cancel ${queries.size} running query execution(s)?")) {
              queries.foreach(queryId => client.cancelQuery(queryId))
            }
          }

        case QueryMethod.Restful | QueryMethod.RestfulV2 =>
          window.alert("You cannot cancel queries when issuing queries through the REST api")
      }

    // --- Build the UI ---

    def stepBackMany(): Unit = updateHistory(_.stepBack(), () => stepBackToCheckpoint())
    def stepForwardMany(): Unit = updateHistory(_.stepForward(), () => stepForwardToCheckpoint())

    div(
      height := "100%",
      width := "100%",
      overflow := "hidden",
      position := "relative",
      onMountCallback { _ =>
        // componentDidMount equivalent
        props.queryMethod match {
          case QueryMethod.WebSocket => getWebSocketClient()
          case QueryMethod.WebSocketV2 => getWebSocketClientV2()
          case _ => ()
        }

        val canView = props.permissions match {
          case Some(perms) => Set("StoredQueryRead", "NodeAppearanceRead").subsetOf(perms)
          case None => true
        }
        if (!canView) () // skip data loading if user lacks permissions
        else
          props.queryMethod match {
            case QueryMethod.WebSocket | QueryMethod.Restful =>
              props.routes
                .queryUiAppearance(())
                .future
                .map(nas => stateVar.update(_.copy(uiNodeAppearances = nas)))
                .onComplete(_ => if (props.initialQuery.nonEmpty) submitQuery(UiQueryType.Node))

              props.routes
                .queryUiSampleQueries(())
                .future
                .foreach(sqs => stateVar.update(_.copy(sampleQueries = sqs)))

              props.routes
                .queryUiQuickQueries(())
                .future
                .foreach(qqs => stateVar.update(_.copy(uiNodeQuickQueries = qqs)))

            case QueryMethod.RestfulV2 | QueryMethod.WebSocketV2 =>
              props.routes
                .queryUiAppearanceV2(())
                .future
                .map(nas => stateVar.update(_.copy(uiNodeAppearances = nas)))
                .onComplete(_ => if (props.initialQuery.nonEmpty) submitQuery(UiQueryType.Node))

              props.routes
                .queryUiSampleQueriesV2(())
                .future
                .foreach(sqs => stateVar.update(_.copy(sampleQueries = sqs)))

              props.routes
                .queryUiQuickQueriesV2(())
                .future
                .foreach(qqs => stateVar.update(_.copy(uiNodeQuickQueries = qqs)))
          }
      },
      // TopBar
      if (props.isQueryBarVisible) {
        TopBar(
          query = stateVar.signal.map(_.query),
          updateQuery = (newQuery: String) => stateVar.update(_.copy(queryBarColor = None, query = newQuery)),
          runningTextQuery = stateVar.signal.map(_.pendingTextQueries.nonEmpty),
          queryBarColor = stateVar.signal.map(_.queryBarColor),
          sampleQueries = stateVar.signal.map(_.sampleQueries),
          foundNodesCount = stateVar.signal.map(_.foundNodesCount),
          foundEdgesCount = stateVar.signal.map(_.foundEdgesCount),
          submitButton = (shiftHeld: Boolean) => submitQuery(if (shiftHeld) UiQueryType.Text else UiQueryType.Node),
          cancelButton = () => cancelQueries(Some(stateVar.now().pendingTextQueries)),
          navButtons = HistoryNavigationButtons(
            canStepBackward = stateVar.signal.map(_.history.canStepBackward),
            canStepForward = stateVar.signal.map(_.history.canStepForward),
            isAnimating = stateVar.signal.map(_.animating),
            undoMany = () => stepBackMany(),
            undo = () => updateHistory(_.stepBack()),
            animate = () => stateVar.update(s => s.copy(animating = !s.animating)),
            redo = () => updateHistory(_.stepForward()),
            redoMany = () => stepForwardMany(),
            makeCheckpoint = () => {
              Option(window.prompt("Name your checkpoint")).foreach { name =>
                networkLayout(() => updateHistory(hist => Some(hist.observe(QueryUiEvent.Checkpoint(name)))))
              }
            },
            checkpointContextMenu = (e: dom.MouseEvent) => {
              e.preventDefault()
              contextMenuVar.set(Some(ContextMenuState(e.pageX, e.pageY, checkpointContextMenuItems())))
            },
            downloadHistory = (snapshotOnly: Boolean) => {
              networkLayout { () =>
                val history = if (snapshotOnly) makeSnapshot() else stateVar.now().history
                downloadHistoryFile(history, if (snapshotOnly) "snapshot.json" else "history.json")
              }
            },
            downloadGraphJsonLd = () => downloadGraphJsonLd(),
            uploadHistory = files => uploadHistory(files),
            atTime = stateVar.signal.map(_.atTime),
            canSetTime = stateVar.signal.map(_.runningQueryCount == 0),
            setTime = setAtTime(_),
            toggleLayout = toggleNetworkLayout,
            recenterViewport = recenterNetworkViewport,
          ),
          permissions = props.permissions,
        )
      } else emptyNode,
      // Loader
      child <-- stateVar.signal.map { s =>
        Loader(
          s.runningQueryCount,
          if (props.queryMethod == QueryMethod.WebSocket || props.queryMethod == QueryMethod.WebSocketV2)
            Some(() => cancelQueries())
          else None,
        )
      },
      // VisNetwork
      VisNetwork(
        data = props.graphData,
        afterNetworkInit = afterNetworkInit,
        clickHandler = _ => contextMenuVar.set(None),
        contextMenuHandler = _.preventDefault(),
        keyDownHandler = networkKeyDown,
        options = networkOptions,
      ),
      // Physics toggle
      stateVar.signal.map(_.animating).updates --> { animating =>
        network.foreach(_.setOptions(new vis.Network.Options {
          override val physics = new vis.Network.Options.Physics {
            override val enabled = animating
          }
        }))
      },
      // MessageBar
      child <-- bottomBarVar.signal.map {
        case Some(content) => MessageBar(content, () => bottomBarVar.set(None))
        case None => emptyNode
      },
      // ContextMenu
      child <-- contextMenuVar.signal.map {
        case Some(ContextMenuState(x, y, items)) => ContextMenu(x, y, items)
        case None => emptyNode
      },
    )
  }

  /** Create a QueryUi from options (replacing makeQueryUi) */
  def fromOptions(
    options: QueryUiOptions,
    routes: ClientRoutes,
    permissions: Option[Set[String]] = None,
  ): HtmlElement = {
    val nodeSet = options.visNodeSet.getOrElse(new vis.DataSet(js.Array[vis.Node]()))
    val edgeSet = options.visEdgeSet.getOrElse(new vis.DataSet(js.Array[vis.Edge]()))
    val visData = new vis.Data {
      override val nodes = nodeSet
      override val edges = edgeSet
    }

    val queryMethod = QueryMethod.parseQueryMethod(options)

    apply(
      Props(
        routes = routes,
        graphData = VisData(visData, nodeSet, edgeSet),
        initialQuery = options.initialQuery.getOrElse(""),
        nodeResultSizeLimit = options.nodeResultSizeLimit.getOrElse(100).toLong,
        onNetworkCreate = options.onNetworkCreate.toOption,
        isQueryBarVisible = options.isQueryBarVisible.getOrElse(true),
        showEdgeLabels = options.showEdgeLabels.getOrElse(true),
        showHostInTooltip = options.showHostInTooltip.getOrElse(true),
        initialAtTime = options.queryHistoricalTime.toOption.map(_.toLong),
        initialLayout = options.layout.getOrElse("graph").toLowerCase match {
          case "tree" => NetworkLayout.Tree
          case "graph" | _ => NetworkLayout.Graph
        },
        queryMethod = queryMethod,
        permissions = permissions,
      ),
    )
  }
}

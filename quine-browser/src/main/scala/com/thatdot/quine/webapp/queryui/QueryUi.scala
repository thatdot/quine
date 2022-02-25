package com.thatdot.quine.webapp.queryui

import scala.collection.mutable
import scala.concurrent._
import scala.scalajs.js
import scala.scalajs.js.Dynamic.{literal => jsObj}
import scala.scalajs.js.|
import scala.util._

import endpoints4s.{Invalid, Valid}
import org.scalajs.dom
import org.scalajs.dom.{document, window}
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._
import slinky.core._
import slinky.core.annotations.react
import slinky.core.facade.ReactElement
import slinky.web.SyntheticKeyboardEvent
import slinky.web.html.{`type` => _, _}

import com.thatdot.quine.Util.{escapeHtml, renderJsonResultValue}
import com.thatdot.quine.routes._
import com.thatdot.quine.webapp.History
import com.thatdot.quine.webapp.components._
import com.thatdot.{visnetwork => vis}

/** Interactive Query UI */
@react class QueryUi extends Component {

  /** @param routes precisely the minimum query API needed to function
    * @param graphData underlying vis.js mutable state
    * @param initialQuery query with which to populate the query bar
    * @param nodeResultSizeLimit threshold at which user gets asked whether to render the results
    * @param hostColors list of colors through which to cycle when rendering nodes
    * @param onNetworkCreate callback that gets executed on network creation
    * @param isQueryBarVisible should the query bar be displayed at all
    * @param initialLayout how is the network initially layed out?
    * @param edgeQueryLanguage which query language is used for edge queries
    * @param queryMethod how should the query UI relay queries to the backend?
    */
  case class Props(
    routes: ClientRoutes,
    graphData: VisData = VisData(Seq.empty, Seq.empty),
    initialQuery: String = "",
    nodeResultSizeLimit: Long = 100,
    hostColors: Vector[String] = Vector("#97c2fc", "green", "purple", "blue", "red", "orange", "yellow", "black"),
    onNetworkCreate: Option[js.Function1[vis.Network, js.Any]] = None,
    isQueryBarVisible: Boolean = true,
    showEdgeLabels: Boolean = true,
    showHostInTooltip: Boolean = true,
    initialAtTime: Option[Long] = None,
    initialLayout: NetworkLayout = NetworkLayout.Graph,
    edgeQueryLanguage: QueryLanguage = QueryLanguage.Gremlin,
    queryMethod: QueryMethod = QueryMethod.WebSocket
  )

  /** @param query input in the query bar
    * @param pendingTextQueries which text queries are currently running?
    * @param queryBarColor color for the query bar
    * @param sampleQueries list of sample queries to show from the query bar
    * @param history UI events that have occurred
    * @param animating is the network in animating mode?
    * @param foundNodesCount counter for nodes returned by a query
    * @param foundEdgesCount counter for edges returned by a query
    * @param runningQueryCount total number of currently running queries
    * @param bottomBar message or results on the bottom of the screen
    * @param contextMenuOpt if open, a context menu
    * @param uiNodeQuickQueries possible set of quick queries
    * @param uiNodeAppearances possible set of node appearances
    * @param atTime possibly historical time (milliseconds since 1970) to query
    */
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
    bottomBar: Option[MessageBarContent],
    contextMenuOpt: Option[ContextMenu.Props],
    uiNodeQuickQueries: Vector[UiNodeQuickQuery],
    uiNodeAppearances: Vector[UiNodeAppearance],
    atTime: Option[Long]
  )

  def initialState: com.thatdot.quine.webapp.queryui.QueryUi.State = State(
    props.initialQuery,
    pendingTextQueries = Set.empty,
    queryBarColor = None,
    sampleQueries = Vector.empty,
    history = History.empty,
    animating = false,
    foundNodesCount = None,
    foundEdgesCount = None,
    runningQueryCount = 0,
    bottomBar = None,
    contextMenuOpt = None,
    uiNodeQuickQueries = UiNodeQuickQuery.defaults,
    uiNodeAppearances = Vector.empty,
    atTime = props.initialAtTime
  )

  private[this] var network: Option[vis.Network] = None
  private[this] var layout: NetworkLayout = NetworkLayout.Graph
  private[this] var webSocketClientFut: Future[WebSocketQueryClient] =
    Future.failed(new Exception("Client not initialized"))

  /** Get a websocket client
    *
    * If the last client is no longer open or the last attempt to open a client failed, try creating
    * a new client and return a future which will complete when the websocket moves out of its
    * `CONNECTING` state.
    */
  def getWebSocketClient(): Future[WebSocketQueryClient] = {
    webSocketClientFut.value match {
      // There is an active existing client
      case Some(Success(client)) if client.webSocket.readyState == dom.WebSocket.OPEN => ()

      // There is already a client in the process of connecting
      case None => ()

      // Either the last client failed, the last client errored, or it has not yet been initialized
      case Some(_) =>
        val client = props.routes.queryProtocolClient()
        val clientReady = Promise[WebSocketQueryClient]()
        val webSocket = client.webSocket

        webSocket.addEventListener[dom.MessageEvent]("open", (_: dom.MessageEvent) => clientReady.trySuccess(client))
        webSocket.addEventListener[dom.Event](
          "error",
          (_: dom.Event) => clientReady.tryFailure(new Exception(s"WebSocket connection to `${webSocket.url}` failed"))
        )
        webSocket.addEventListener[dom.CloseEvent](
          "close",
          (_: dom.CloseEvent) =>
            clientReady.tryFailure(new Exception(s"WebSocket connection to `${webSocket.url}` was closed"))
        )

        webSocketClientFut = clientReady.future
    }
    webSocketClientFut
  }

  /** For a node, compute its relevant quick queries
    *
    * @param node node and properties
    * @return list of all the quick queries that apply to the node
    */
  def quickQueriesFor(node: UiNode[String]): Seq[QuickQuery] =
    state.uiNodeQuickQueries.collect { case UiNodeQuickQuery(p, qq) if p.matches(node) => qq }

  /** For a node, compute its `vis` UI label and appearance
    *
    * @note the label here describes the text under node displayed in the UI. This is not the same
    * thing as the label on `UiNode`, which represents the DB notion of a label. This tends to be
    * confusing because the default UI label is the DB label.
    *
    * @param node node and properties
    * @return node `vis` label to display and its style
    */
  def appearanceFor(node: UiNode[String]): (String, vis.NodeOptions.Icon) = {

    val (sizeOpt, iconOpt, colorOpt, labelDescOpt) = state.uiNodeAppearances
      .find(_.predicate.matches(node))
      .map(appearance => (appearance.size, appearance.icon, appearance.color, appearance.label))
      .getOrElse((None, None, None, None))

    /* We need to specify explicitly the icon code and size defaults instead of just relying on a
     * global default because once you override the default, there is no way to update the dataset
     * to switch back to the  default (the `vis.DataSet` function to `update` only merges new
     * properties - it doesn't remove old ones). An example of when this is problematic is when
     * a node with a custom appearance gets cleared and we need its rendering to be the "default"
     * again.
     */
    val visIcon = new vis.NodeOptions.Icon {
      override val color =
        colorOpt.getOrElse[String](props.hostColors(Math.floorMod(node.hostIndex, props.hostColors.length)))
      override val code = iconOpt.getOrElse[String]("\uf3a6")
      override val size = sizeOpt.getOrElse[Double](30.0)
    }

    val uiLabel = labelDescOpt match {
      case Some(UiNodeLabel.Constant(lbl)) => lbl
      case Some(UiNodeLabel.Property(key, prefix)) if node.properties.contains(key) =>
        val propVal = node.properties(key) match {
          case ujson.Str(str) => str
          case other => ujson.write(other)
        }
        prefix.getOrElse("") + propVal
      case _ => node.label
    }

    uiLabel -> visIcon
  }

  override def componentDidMount(): Unit = {
    // If the layout is not graph, toggle it
    if (props.initialLayout != NetworkLayout.Graph) toggleNetworkLayout()

    // If the query mode is websockets, start connecting one
    if (props.queryMethod == QueryMethod.WebSocket) getWebSocketClient()

    // Populate node appearances, and only after that submit a possible initial query
    props.routes
      .queryUiAppearance(())
      .future
      .map(nas => setState(_.copy(uiNodeAppearances = nas)))
      .onComplete(_ => if (props.initialQuery.nonEmpty) submitQuery(UiQueryType.Node))

    // Populate the sample queries
    props.routes
      .queryUiSampleQueries(())
      .future
      .foreach(sqs => setState(_.copy(sampleQueries = sqs)))

    // Populate quick queries
    props.routes
      .queryUiQuickQueries(())
      .future
      .foreach(qqs => setState(_.copy(uiNodeQuickQueries = qqs)))
  }

  /** Convert a [[UiNode]] to a `vis` one
    *
    * @param node internal node representation
    * @param startingPosition hint for where the node should be placed
    */
  def nodeUi2Vis(
    node: UiNode[String],
    startingPosition: Option[(Double, Double)]
  ): vis.Node = {
    val (uiLabel, iconStyle) = appearanceFor(node)

    new QueryUiVisNodeExt {
      override val id = node.id
      override val label = uiLabel
      override val icon = iconStyle
      override val uiNode = node

      override val x = startingPosition match {
        case Some((x, _)) => x
        case None => js.undefined
      }
      override val y = startingPosition match {
        case Some((_, y)) => y
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
            s"${escapeHtml(keyStr)} : ${escapeHtml(ujson.write(valueJson))}"
          }

        (idProp :: hostProp ++ strProps).mkString("<br>")
      }
    }
  }

  /** Deterministic ID computed from an edge */
  private def edgeId(edge: UiEdge[String]): String =
    s"${edge.from}-${edge.edgeType}->${edge.to}"

  /** Convert a [[UiEdge]] to a `vis` one
    *
    * @param edge internal edge representation
    * @param isSynEdge is this edge the result of a quick query?
    */
  def edgeUi2Vis(
    edge: UiEdge[String],
    isSynEdge: Boolean = false
  ): vis.Edge = new QueryUiVisEdgeExt {
    override val id = edgeId(edge)
    override val from = edge.from
    override val to = edge.to
    override val label = if (props.showEdgeLabels) edge.edgeType else js.undefined
    override val arrows = if (edge.isDirected) "to" else ""
    override val uiEdge = edge
    override val isSyntheticEdge = isSynEdge
    override val color = if (isSynEdge) "purple" else js.undefined
    override val dashes = if (isSynEdge) true else js.undefined
  }

  /** Typeclass instance that defines how query UI events can be applied to
    * produce side-effects
    */
  implicit val queryUiEvent: History.Event[QueryUiEvent] = new History.Event[QueryUiEvent] {
    import QueryUiEvent._
    import js.JSConverters._

    def applyEvent(event: QueryUiEvent): Unit = event match {
      case Add(nodes, edges, updateNodes, syntheticEdges, explodeFromId) =>
        window.setTimeout(() => animateNetwork(), 0)

        // Position from quick query
        val posOpt: Option[(Double, Double)] = explodeFromId.map { startingId =>
          val bb = network.get.getBoundingBox(startingId)
          ((bb.left + bb.right) / 2, (bb.top + bb.bottom) / 2)
        }

        props.graphData.nodeSet.add(nodes.map(nodeUi2Vis(_, posOpt)).toJSArray)
        props.graphData.edgeSet.add(edges.map(edgeUi2Vis(_, false)).toJSArray)
        props.graphData.edgeSet.add(syntheticEdges.map(edgeUi2Vis(_, true)).toJSArray)
        props.graphData.nodeSet.update(updateNodes.map(nodeUi2Vis(_, None)).toJSArray)

        /* Yes, this looks completely redundant since we never change these
         * options, but for some crazy reason it isn't: this ensures that nodes
         * are always rendered with an icon (they previously weren't when in
         * tree layout)
         */
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

        ()

      case Collapse(nodes, clusterId, name) =>
        val nodeIds: Set[vis.IdType] = nodes.map(id => id: vis.IdType).toSet
        window.setTimeout(() => animateNetwork(), 0)
        network.get.cluster(new vis.ClusterOptions {
          override val joinCondition = Some[js.Function1[js.Any, Boolean]]((n: js.Any) =>
            nodeIds.contains(n.asInstanceOf[vis.Node].id)
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
        window.setTimeout(() => animateNetwork(), 0)
        network.get.openCluster(clusterId)

      case Checkpoint(_) =>
      case Layout(positions) =>
        for ((nodeId, NodePosition(x, y, isFixed)) <- positions) {
          network.get.moveNode(nodeId, x, y)
          props.graphData.nodeSet.update(new vis.Node {
            override val id = nodeId
            override val fixed = isFixed
            override val shadow = isFixed
          })
        }
    }

    def invert(event: QueryUiEvent) = event.invert
  }

  /** Change the history state using a function
    *
    * @param update change the history using this function
    * @param callback what to do after the history is updated
    */
  def updateHistory(
    update: History[QueryUiEvent] => Option[History[QueryUiEvent]],
    callback: () => Unit = () => ()
  ): Unit =
    setState(
      s => update(s.history).fold(s)(h => s.copy(history = h)),
      callback
    )

  /** Download a history file */
  def downloadHistory(history: History[QueryUiEvent], fileName: String): Unit = {
    val blob = new dom.Blob(
      js.Array(HistoryJsonSchema.encode(history)),
      new dom.BlobPropertyBag { `type` = "application/json" }
    )

    val a = document.createElement("a").asInstanceOf[dom.HTMLAnchorElement]
    a.setAttribute("download", fileName)
    a.setAttribute("href", dom.URL.createObjectURL(blob))
    a.setAttribute("target", "_blank")
    a.click()
  }

  /** Upload a history file and modify the history */
  def uploadHistory(files: dom.FileList): Unit = {
    // Assert only one file
    val file = if (files.length != 1) {
      val msg = s"Expected one file, but got ${files.length}"
      setState(_.copy(bottomBar = Some(MessageBarContent(pre(msg), "pink"))))
      return
    } else {
      files(0)
    }

    // Assert JSON file
    if (file.`type` != "application/json") {
      val msg = s"Expected JSON file, but `${file.name}' has type '${file.`type`}'."
      setState(_.copy(bottomBar = Some(MessageBarContent(pre(msg), "pink"))))
      return
    }

    val reader = new dom.FileReader()
    reader.onload = (e: dom.ProgressEvent) => {
      // Deserialize
      val jsonStr = e.target.asInstanceOf[dom.FileReader].result.asInstanceOf[String]
      HistoryJsonSchema.decode(jsonStr) match {
        case Valid(hist) =>
          val msg = """Uploading this history will erase your existing one.
                      |
                      |Do you wish to continue?""".stripMargin
          if (window.confirm(msg)) {
            setState(
              _.copy(history = hist),
              () => hist.past.reverse.foreach(queryUiEvent.applyEvent(_))
            )
          }

        case Invalid(errs) =>
          val msg = s"Malformed JSON history file:${errs.mkString("\n  ", "\n  ", "")}"
          setState(_.copy(bottomBar = Some(MessageBarContent(pre(msg), "pink"))))
      }
    }
    reader.readAsText(file)
  }

  /** Produce the SVG source for a snapshot of the current graph */
  def downloadSvgSnapshot(fileName: String = "graph.svg"): Unit = {
    val positions = network.get.getPositions(props.graphData.nodeSet.getIds())
    SvgSnapshot(props.graphData, positions).map { svgGraph =>
      val tempElement = document.createElement("div")
      slinky.web.ReactDOM.render(svgGraph, tempElement)

      val blob = new dom.Blob(
        js.Array(tempElement.innerHTML),
        new dom.BlobPropertyBag { `type` = "image/svg" }
      )

      val a = document.createElement("a").asInstanceOf[dom.HTMLAnchorElement]
      a.setAttribute("download", fileName)
      a.setAttribute("href", dom.URL.createObjectURL(blob))
      a.setAttribute("target", "_blank")
      a.click()
    }
    ()
  }

  /** Synthesize a flat history from the current state */
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
      future = List()
    )
  }

  private val cypherQueryRegex = js.RegExp(
    raw"^\s*(optional|match|return|unwind|create|foreach|merge|call|load|with|explain)[^a-z]",
    flags = "i"
  )

  /** Try to guess the query language based on a query string */
  private def guessQueryLanguage(query: String): QueryLanguage = cypherQueryRegex.test(query) match {
    case true => QueryLanguage.Cypher
    case false => QueryLanguage.Gremlin
  }

  private def mergeEndpointErrorsIntoFuture[A](fut: Future[Either[Invalid, A]]): Future[A] =
    fut.flatMap {
      case Left(Invalid(errs)) => Future.failed(new Exception(errs.mkString("\n")))
      case Right(success) => Future.successful(success)
    }

  /** Issue a query that returns nodes and get back results in one final batch
    *
    * @return query results ([[None]] means the query was cancelled)
    */
  private def nodeQuery(
    query: String,
    atTime: Option[Long],
    language: QueryLanguage,
    parameters: Map[String, ujson.Value]
  ): Future[Option[Seq[UiNode[String]]]] =
    props.queryMethod match {
      case QueryMethod.Restful =>
        mergeEndpointErrorsIntoFuture(language match {
          case QueryLanguage.Gremlin =>
            props.routes.gremlinNodesPost((atTime, None, GremlinQuery(query))).future
          case QueryLanguage.Cypher =>
            props.routes.cypherNodesPost((atTime, None, CypherQuery(query))).future
        }).map(Some(_))

      case QueryMethod.WebSocket =>
        val nodeCallback = new QueryCallbacks.CollectNodesToFuture()
        val streamingQuery = StreamingQuery(query, parameters, language, atTime, None, Some(100))
        for {
          client <- getWebSocketClient()
          queryId <- Future.fromTry(client.query(streamingQuery, nodeCallback).toTry)
          results <- nodeCallback.future
        } yield results
    }

  /** Issue a query that returns edges and get back results in one final batch
    *
    * @return query results ([[None]] means the query was cancelled)
    */
  private def edgeQuery(
    query: String,
    atTime: Option[Long],
    language: QueryLanguage,
    parameters: Map[String, ujson.Value]
  ): Future[Option[Seq[UiEdge[String]]]] =
    props.queryMethod match {
      case QueryMethod.Restful =>
        mergeEndpointErrorsIntoFuture(language match {
          case QueryLanguage.Gremlin =>
            props.routes.gremlinEdgesPost((atTime, None, GremlinQuery(query, parameters))).future
          case QueryLanguage.Cypher =>
            props.routes.cypherEdgesPost((atTime, None, CypherQuery(query, parameters))).future
        }).map(Some(_))

      case QueryMethod.WebSocket =>
        val edgeCallback = new QueryCallbacks.CollectEdgesToFuture()
        val streamingQuery = StreamingQuery(query, parameters, language, atTime, None, Some(100))
        for {
          client <- getWebSocketClient()
          queryId <- Future.fromTry(client.query(streamingQuery, edgeCallback).toTry)
          results <- edgeCallback.future
        } yield results
    }

  /** Issue a query that returns text results and get back a batch of results
    *
    * @param updateResults callback signalling this is the new "latest" results
    * @return the query ID and the query results ([[None]] means the query was cancelled)
    */
  private def textQuery(
    query: String,
    atTime: Option[Long],
    language: QueryLanguage,
    parameters: Map[String, ujson.Value],
    updateResults: Either[Seq[ujson.Value], CypherQueryResult] => Unit
  ): Future[Option[Unit]] =
    (props.queryMethod, language) match {
      case (QueryMethod.Restful, QueryLanguage.Gremlin) =>
        val gremlinResults = props.routes.gremlinPost((atTime, None, GremlinQuery(query, parameters))).future
        mergeEndpointErrorsIntoFuture(gremlinResults).map { results =>
          updateResults(Left(results))
          Some(())
        }

      case (QueryMethod.Restful, QueryLanguage.Cypher) =>
        val cypherResults = props.routes.cypherPost((atTime, None, CypherQuery(query, parameters))).future
        mergeEndpointErrorsIntoFuture(cypherResults).map { results =>
          updateResults(Right(results))
          Some(())
        }

      case (QueryMethod.WebSocket, _) =>
        val result = Promise[Option[Unit]]()

        val textCallback: QueryCallbacks = language match {
          case QueryLanguage.Gremlin =>
            new QueryCallbacks.NonTabularCallbacks {
              private var buffered = Seq.empty[ujson.Value]
              private var cancelled = false

              def onNonTabularResults(batch: Seq[ujson.Value]): Unit = {
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
                columns: Option[Seq[String]]
              ): Unit = ()

              def onQueryCancelOk(): Unit = cancelled = true
              def onQueryCancelError(message: String): Unit = ()
            }

          case QueryLanguage.Cypher =>
            new QueryCallbacks.TabularCallbacks {
              private var buffered = Seq.empty[Seq[ujson.Value]]
              private var cancelled = false

              def onTabularResults(columns: Seq[String], batch: Seq[Seq[ujson.Value]]): Unit = {
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
                columns: Option[Seq[String]]
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
            setState(s => s.copy(pendingTextQueries = s.pendingTextQueries + queryId))
            result.future.onComplete { _ =>
              setState(s => s.copy(pendingTextQueries = s.pendingTextQueries - queryId))
            }
          }
          results <- result.future
        } yield results
    }

  // Syntactic sugar for requesting to observe a SQ
  private[this] val ObserveStandingQuery = "(?:OBSERVE|observe) [\"']?(.*)[\"']?".r

  /** Submit the query that is currently loaded up */
  def submitQuery(uiQueryType: UiQueryType): Unit = {

    val query = state.query match {
      case ObserveStandingQuery(sqName) =>
        val amendedQuery = s"CALL standing.wiretap({ name: '$sqName' })"
        setState(_.copy(query = amendedQuery))
        amendedQuery
      case other =>
        other
    }
    val language = guessQueryLanguage(query)

    if (state.pendingTextQueries.nonEmpty) {
      window.alert(
        """You have a pending text query. You must cancel it before issuing another query.
          |Pending queries can be cancelled by clicking on the spinning loader in the top right.
          |""".stripMargin
      )
      return
    }

    setState(s =>
      s.copy(
        foundNodesCount = None,
        foundEdgesCount = None,
        bottomBar = None,
        queryBarColor = None,
        runningQueryCount = s.runningQueryCount + 1
      )
    )

    if (uiQueryType == UiQueryType.Text) {

      def updateResults(result: Either[Seq[ujson.Value], CypherQueryResult]): Unit = {
        val rendered: ReactElement = result match {
          case Left(results) => pre(renderJsonResultValue(ujson.Arr(results: _*)))
          case Right(results) => CypherResultsTable(results)
        }
        setState(_.copy(bottomBar = Some(MessageBarContent(rendered, "lightgreen"))))
      }

      textQuery(query, state.atTime, language, Map.empty, updateResults).onComplete {
        case Success(outcome) =>
          val outcomeColor = if (outcome == None) "lightgrey" else "lightgreen"
          setState(s =>
            s.copy(
              queryBarColor = Some(outcomeColor),
              bottomBar = s.bottomBar.map {
                case MessageBarContent(res, "lightgreen") => MessageBarContent(res, outcomeColor)
                case other => other
              },
              runningQueryCount = s.runningQueryCount - 1
            )
          )
          window.setTimeout(
            () => setState(_.copy(queryBarColor = None)),
            750
          )
          ()

        case Failure(err) =>
          val failureBar = MessageBarContent(pre(err.getMessage), "pink")
          setState(s =>
            s.copy(
              queryBarColor = Some("pink"),
              bottomBar = Some(failureBar),
              runningQueryCount = s.runningQueryCount - 1
            )
          )
      }

    } else {

      val nodesEdgesFut = for {
        // Query for nodes
        rawNodesOpt <- nodeQuery(query, state.atTime, language, Map.empty)
        dedupedNodesOpt = rawNodesOpt.map { rawNodes =>
          val dedupedIds = mutable.Set.empty[String]
          rawNodes.filter(n => dedupedIds.add(n.id))
        }

        // Update the node counter, avoid accidentally rendering too many nodes
        nodes = dedupedNodesOpt match {
          case Some(dedupedNodes) if dedupedNodes.length > props.nodeResultSizeLimit =>
            val limitedCount: Option[Int] = Option(
              window.prompt(
                s"You are about to render ${dedupedNodes.length} nodes.\nHow many do you want to render?",
                dedupedNodes.length.toString
              )
            ).map(_.toInt)
            setState(s => s.copy(foundNodesCount = limitedCount))
            limitedCount.fold(Seq.empty[UiNode[String]])(dedupedNodes.take(_))

          case Some(dedupedNodes) =>
            setState(s => s.copy(foundNodesCount = Some(dedupedNodes.length)))
            dedupedNodes

          case None =>
            Nil
        }

        // Query for edges
        newNodes = nodes.map(_.id).toSet
        edgesOpt <-
          if (newNodes.isEmpty) {
            Future.successful(Some(Nil)) // Don't bother with an edge query if there are no new nodes
          } else {
            val query = props.edgeQueryLanguage match {
              case QueryLanguage.Gremlin =>
                """g.V(new).bothE().dedup().where(_.and(
                | _.outV().strId().is(within(all)),
                | _.inV().strId().is(within(all))))""".stripMargin

              case QueryLanguage.Cypher =>
                """UNWIND $new AS newId
                |MATCH (n)-[e]-(m)
                |WHERE strId(n) = newId AND strId(m) IN $all
                |RETURN DISTINCT e""".stripMargin
            }
            val existingNodes = props.graphData.nodeSet.getIds().map(_.toString).toVector
            val queryParameters = Map(
              "new" -> ujson.Arr.from(newNodes.map(ujson.Str(_))),
              "all" -> ujson.Arr.from((existingNodes ++ newNodes).map(ujson.Str(_)))
            )
            edgeQuery(query, state.atTime, props.edgeQueryLanguage, queryParameters)
          }
        edges = edgesOpt match {
          case Some(edges) =>
            if (rawNodesOpt.nonEmpty) setState(s => s.copy(foundEdgesCount = Some(edges.length)))
            edges
          case None =>
            Nil
        }
      } yield (nodes, edges)

      nodesEdgesFut.onComplete {
        case Success((nodes, edges)) =>
          // Add indirect edges required by some quick queries
          val (syntheticEdges, explodeFromIdOpt) = uiQueryType match {
            case UiQueryType.NodeFromId(explodeFromId, Some(syntheticEdgeLabel)) =>
              nodes.map(n => UiEdge(explodeFromId, syntheticEdgeLabel, n.id)) -> Some(explodeFromId)
            case UiQueryType.NodeFromId(explodeFromId, None) =>
              (Seq.empty, Some(explodeFromId))
            case UiQueryType.Node | UiQueryType.Text =>
              (Seq.empty, None)
          }

          // `vis` crashes if we try to add something already in the set
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
            explodeFromIdOpt
          )

          // Update the history
          if (addEvent.nonEmpty) {
            updateHistory(hist => Some(hist.observe(addEvent)))
          }
          setState(s => s.copy(runningQueryCount = s.runningQueryCount - 1))

        case Failure(err) =>
          val message = err.getMessage
          val contents = Seq.newBuilder[ReactElement]
          contents += pre(className := "wrap")(if (message.isEmpty) "Cannot connect to server" else message)
          if (message.startsWith("TypeMismatchError Expected type(s) Node but got value")) {
            val failedQuery = state.query
            contents += a(
              onClick := { () =>
                setState(
                  _.copy(
                    query = failedQuery
                  )
                )
                submitQuery(UiQueryType.Text)
              },
              href := "#"
            )("Run again as text query")
          }
          setState(s =>
            s.copy(
              queryBarColor = Some("pink"),
              bottomBar = Some(
                MessageBarContent(
                  div(
                    contents.result()
                  ),
                  "pink"
                )
              ),
              runningQueryCount = s.runningQueryCount - 1
            )
          )
      }
    }
  }

  private val networkOptions = new vis.Network.Options {
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

  /** Toggle the current layout (tree to/from graph) */
  val toggleNetworkLayout: () => Unit = { () =>
    layout match {
      case NetworkLayout.Graph =>
        layout = NetworkLayout.Tree
        network.get.setOptions(
          new vis.Network.Options {
            override val layout = new vis.Network.Options.Layout {
              override val hierarchical = jsObj(
                enabled = true,
                sortMethod = "directed",
                shakeTowards = "roots"
              )
            }
          }
        )

      case NetworkLayout.Tree =>
        layout = NetworkLayout.Graph
        network.get.setOptions(
          new vis.Network.Options {
            override val layout = new vis.Network.Options.Layout {
              override val hierarchical = false: js.Any
            }
          }
        )

        /* Yes, this looks completely redundant with the above, but for some
         * crazy reason it isn't: this ensures that edges stay rigid. Resist
         * the temptation to re-use `networkOptions.edges` too - that doesn't
         * work either.
         */
        network.get.setOptions(new vis.Network.Options {
          override val edges = new vis.EdgeOptions {
            override val smooth = false
            override val arrows = "to"
          }
        })
    }
    animateNetwork()
  }

  /** Return to the center of the canvas */
  val recenterNetworkViewport: () => Unit =
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
        }
      )

  /** Animate the network for some milliseconds */
  def animateNetwork(millis: Double = 1000): Unit =
    setState(
      _.copy(animating = true),
      () => {
        window.setTimeout(() => setState(_.copy(animating = false)), millis)
        ()
      }
    )

  def networkHold(event: vis.ClickEvent): Unit = {

    // Only if there was a node that was held do we proceed
    val heldNodeId = network.get.getNodeAt(event.pointer.DOM).toOption match {
      case None => return
      case Some(id) => id
    }

    /* If the shift key is held, we toggle whether the selected node is "fixed".
     * Since multiple nodes can be selected, we must handle the case where only
     * part of the selected nodes are "fixed" (so there is no obvious toggle).
     * We choose to interpret this case as "fix nodes that aren't already fixed"
     */
    if (event.event.asInstanceOf[VisIndirectMouseEvent].srcEvent.shiftKey) {
      val TrueFixed: Boolean | vis.NodeOptions.Fixed = true

      // Undo the default behaviour of unselecting the held node
      val selectedNodes = network.get.getSelectedNodes()
      network.get.selectNodes(heldNodeId +: selectedNodes)

      // Split selected nodes into those already fixed and those not
      val (fixedNodes, unfixedNodes) = (heldNodeId +: event.nodes)
        .map(nodeId => (props.graphData.nodeSet.get(nodeId).merge: vis.Node))
        .partition(_.fixed.toOption.fold(false) {
          case TrueFixed => true
          case _ => false
        })

      // Generate an array for `vis` to fix/unfix nodes and turn them black
      val (fixedAndFlashNodes, flashedNodes) = if (unfixedNodes.isEmpty) {
        val unfixNodesUpdate = fixedNodes.map(fixedNode =>
          new vis.Node {
            override val id = fixedNode.id
            override val fixed = false
            override val shadow = false
            override val icon = new vis.NodeOptions.Icon {
              override val color = "black"
            }
          }
        )
        unfixNodesUpdate -> fixedNodes
      } else {
        val fixNodesUpdate = unfixedNodes.map(unfixedNode =>
          new vis.Node {
            override val id = unfixedNode.id
            override val fixed = true
            override val shadow = true
            override val icon = new vis.NodeOptions.Icon {
              override val color = "black"
            }
          }
        )
        fixNodesUpdate -> unfixedNodes
      }

      // Generate an array for `vis` to restore nodes' original color
      val unflashNodes = flashedNodes.map(flashedNode =>
        new vis.Node {
          override val id = flashedNode.id
          override val icon = flashedNode.icon
        }
      )

      // Update the graph!
      props.graphData.nodeSet.update(fixedAndFlashNodes)
      window.setTimeout(() => props.graphData.nodeSet.update(unflashNodes), 500)
      ()
    } else {
      val visNode: vis.Node = props.graphData.nodeSet.get(heldNodeId).merge
      val uiNode = visNode.asInstanceOf[QueryUiVisNodeExt].uiNode
      println(uiNode)
      props.routes.literalDebug(uiNode.id -> None).future.onComplete(println(_))
    }
  }

  def networkDeselect(event: vis.DeselectEvent): Unit = {

    // Only re-select the node if SHIFT was held
    if (!event.event.asInstanceOf[VisIndirectMouseEvent].srcEvent.shiftKey)
      return

    // Pull out the node that was clicked
    val clickedId = network.get.getNodeAt(event.pointer.DOM).toOption match {
      case None => return
      case Some(id) => id
    }

    val selection = event.previousSelection.nodes
    if (event.previousSelection.nodes.contains(clickedId)) {
      network.get.selectNodes(selection.filter(_ != clickedId))
    } else {
      network.get.selectNodes(selection :+ clickedId)
    }
  }

  /** Produce the list of items in the context menu
    *
    * @param nodeId the node on which the action was triggered
    * @param selectedIds other nodes that are also selected
    * @return actions to put in a context menu
    */
  def getContextMenuItems(nodeId: String, selectedIds: Seq[String]): Seq[ContextMenuItem] = {

    val contextMenuItems = Seq.newBuilder[ContextMenuItem]

    if (network.get.isCluster(nodeId)) {

      // Expand the cluster by finding the collapse event and inverting it
      val expandOpt = state.history.past
        .find {
          case QueryUiEvent.Collapse(_, cId, _) => cId == nodeId
          case _ => false
        }
        .map(queryUiEvent.invert)

      contextMenuItems ++= expandOpt.toList.map { expand =>
        ContextMenuItem(
          item = "Expand cluster",
          title = "Expand cluster back into nodes",
          action = () =>
            setState(
              _.copy(contextMenuOpt = None),
              () => updateHistory(hist => Some(hist.observe(expand)))
            )
        )
      }
    } else {

      // These are the node(s) on which the quick query runs
      val startingNodes: Seq[String] = if (selectedIds.contains(nodeId)) selectedIds else Seq(nodeId)

      // Lookup the quick queries for the starting nodes
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
              setState(
                _.copy(query = qq.fullQuery(startingNodes), contextMenuOpt = None),
                () => submitQuery(queryType)
              )
            }
          )
        }
    }

    if (selectedIds.length > 1) {

      // Create a cluster, prompting the user for its name
      contextMenuItems += ContextMenuItem(
        item = "Collapse selected nodes",
        title = "Create a cluster node from selected nodes",
        action = () =>
          setState(
            _.copy(contextMenuOpt = None),
            () =>
              Option(window.prompt("Name your cluster:")).foreach { name =>
                updateHistory { hist =>
                  val clusterId = "CLUSTER-" + Random.nextInt().toString.take(10)
                  val clusterEvent = QueryUiEvent.Collapse(selectedIds, clusterId, name)
                  Some(hist.observe(clusterEvent))
                }
              }
          )
      )
    }

    contextMenuItems.result()
  }

  def networkRightClick(event: vis.ClickEvent): Unit = {

    // Pull out the node that was right clicked
    val rightClickedId = network.get.getNodeAt(event.pointer.DOM).toOption match {
      case None => return
      case Some(id) => id.asInstanceOf[String]
    }

    val contextMenuItems = getContextMenuItems(
      rightClickedId,
      event.nodes.toSeq.asInstanceOf[Seq[String]]
    )

    setState(
      _.copy(
        contextMenuOpt = Some(
          ContextMenu.Props(
            x = event.pointer.DOM.x,
            y = event.pointer.DOM.y,
            items = contextMenuItems
          )
        )
      )
    )
  }

  def networkDoubleClick(event: vis.ClickEvent): Unit = {

    // Pull out the ID that was clicked
    val clickedId: String = if (event.nodes.length == 1) {
      event.nodes(0).asInstanceOf[String]
    } else {
      return
    }

    // The action is equivalent to clicking the top item in the context menu
    val contextMenuItems = getContextMenuItems(clickedId, Seq.empty)
    contextMenuItems.headOption.foreach(_.action())
  }

  def networkKeyDown(event: SyntheticKeyboardEvent[dom.HTMLDivElement]): Unit =
    // Intercept delete and backspace so they delete selected nodes
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

  /** Register a new event to layout the nodes in a certain way */
  def networkLayout(callback: () => Unit = () => ()): Unit = {
    val positions = Map.newBuilder[String, QueryUiEvent.NodePosition]
    for ((nodeId, pos) <- network.get.getPositions(props.graphData.nodeSet.getIds())) {
      val visNode: vis.Node = props.graphData.nodeSet.get(nodeId).merge

      val TrueFixed: Boolean | vis.NodeOptions.Fixed = true
      val isFixed = visNode.fixed.toOption.fold(false) {
        case TrueFixed => true
        case _ => false
      }

      positions += nodeId -> QueryUiEvent.NodePosition(pos.x, pos.y, isFixed)
    }

    val layoutEvent = QueryUiEvent.Layout(positions.result())
    updateHistory(hist => Some(hist.observe(layoutEvent)), callback)
  }

  def afterNetworkInit(network: vis.Network): Unit = {
    import vis._

    this.network = Some(network)
    props.onNetworkCreate.foreach(func => func(network))

    network.onDoubleClick(networkDoubleClick)
    network.onContext(networkRightClick)
    network.onHold(networkHold)
    network.onDeselectNode(networkDeselect)
  }

  /** Keep stepping back until the most recent past event is a checkpoint with
    * the right name (or we are at the beginning of the history)
    *
    * @param name name of the checkpoint, or any checkpoint name if `None`
    */
  def stepBackToCheckpoint(name: Option[String] = None): Unit =
    state.history.past match {
      case Nil =>
      case QueryUiEvent.Checkpoint(n) :: _ if name.forall(_ == n) =>
      case _ => updateHistory(_.stepBack(), () => stepBackToCheckpoint(name))
    }

  /** Keep stepping forward until the most recent past event is a checkpoint
    * with the right name (or we are at the end of the history)
    *
    * @param name name of the checkpoint, or any checkpoint name if `None`
    */
  def stepForwardToCheckpoint(name: Option[String] = None): Unit =
    state.history.future match {
      case Nil =>
      case QueryUiEvent.Checkpoint(n) :: _ if name.forall(_ == n) => updateHistory(_.stepForward())
      case _ => updateHistory(_.stepForward(), () => stepForwardToCheckpoint(name))
    }

  /** When right-clicking on the checkpoint button, produce a list of the
    * available checkpoints and the action to jump to them
    */
  def checkpointContextMenuItems(): Seq[ContextMenuItem] = {
    val contextItems = Seq.newBuilder[ContextMenuItem]

    for (event <- state.history.future.reverse)
      event match {
        case QueryUiEvent.Checkpoint(name) =>
          val stepForward =
            () => setState(_.copy(contextMenuOpt = None), () => stepForwardToCheckpoint(Some(name)))
          contextItems += ContextMenuItem(div(name, em(" (future)")), name, stepForward)

        case _ =>
      }

    for ((event, idx) <- state.history.past.zipWithIndex)
      event match {
        case QueryUiEvent.Checkpoint(name) =>
          val item = div(name, em(if (idx == 0) " (present)" else " (past)"))
          val stepBack =
            () => setState(_.copy(contextMenuOpt = None), () => stepBackToCheckpoint(Some(name)))
          contextItems += ContextMenuItem(item, name, stepBack)

        case _ =>
      }

    contextItems.result()
  }

  /** Set the moment in time (or moving present) that should be tracked by the query UI.
    *
    * TODO: cancel queries when switching time
    *
    * @note this involves wiping almost all the state clean!
    * @param atTime new time to track ([[None]] is the present)
    */
  def setAtTime(atTime: Option[Long]): Unit =
    if (atTime != state.atTime) {
      props.graphData.nodeSet.remove(props.graphData.nodeSet.getIds())
      props.graphData.edgeSet.remove(props.graphData.edgeSet.getIds())
      setState(
        _.copy(
          queryBarColor = None,
          history = History.empty,
          animating = false,
          foundNodesCount = None,
          foundEdgesCount = None,
          bottomBar = None,
          contextMenuOpt = None,
          atTime = atTime
        )
      )
    }

  /** Cancel currently running queries
    *
    * TODO: provide a more dire warning if the queries are not read-only
    *
    * @param queryIds which queries to cancel
    */
  def cancelQueries(queryIds: Option[Iterable[QueryId]] = None): Unit =
    props.queryMethod match {
      case QueryMethod.WebSocket =>
        getWebSocketClient().value.flatMap(_.toOption).foreach { (client: WebSocketQueryClient) =>
          val queries = queryIds.getOrElse(client.activeQueries.keys).toList
          if (queries.nonEmpty && window.confirm(s"Cancel ${queries.size} running query execution(s)?")) {
            queries.foreach(queryId => client.cancelQuery(queryId))
          }
        }

      case QueryMethod.Restful =>
        window.alert("You cannot cancel queries when issuing queries throught the REST api")
    }

  def render(): ReactElement = {
    def when[A](cond: Boolean, a: => A): Option[A] = if (cond) Some(a) else None
    val elements = Seq.newBuilder[ReactElement]

    def stepBackMany = () => updateHistory(_.stepBack(), () => stepBackToCheckpoint())
    def stepForwardMany = () => updateHistory(_.stepForward(), () => stepForwardToCheckpoint())

    if (props.isQueryBarVisible) {
      val pendingTextQueries = state.pendingTextQueries
      elements += TopBar(
        state.query,
        pendingTextQueries.nonEmpty,
        state.queryBarColor,
        state.sampleQueries,
        state.foundNodesCount,
        state.foundEdgesCount,
        (newQuery: String) => setState(s => s.copy(queryBarColor = None, query = newQuery)),
        (shiftHeld: Boolean) => submitQuery(if (shiftHeld) UiQueryType.Text else UiQueryType.Node),
        () => cancelQueries(Some(pendingTextQueries)),
        HistoryNavigationButtons.Props(
          undoMany = when(state.history.canStepBackward, stepBackMany),
          undo = when(state.history.canStepBackward, () => updateHistory(_.stepBack())),
          isAnimating = state.animating,
          animate = () => setState(s => s.copy(animating = !s.animating)),
          redo = when(state.history.canStepForward, () => updateHistory(_.stepForward())),
          redoMany = when(state.history.canStepForward, stepForwardMany),
          makeCheckpoint = () => {
            Option(window.prompt("Name your checkpoint")).foreach { name =>
              networkLayout(() => updateHistory(hist => Some(hist.observe(QueryUiEvent.Checkpoint(name)))))
            }
          },
          checkpointContextMenu = e => {
            e.preventDefault()
            // TODO: `pageX` and `pageY` are wrong when not in full screen
            val newContextMenu = ContextMenu.Props(e.pageX, e.pageY, checkpointContextMenuItems())
            setState(_.copy(contextMenuOpt = Some(newContextMenu)))
          },
          downloadHistory = (snapshotOnly: Boolean) => {
            networkLayout { () =>
              val history = if (snapshotOnly) makeSnapshot() else state.history
              downloadHistory(history, if (snapshotOnly) "snapshot.json" else "history.json")
            }
          },
          uploadHistory = e => uploadHistory(e.target.files),
          atTime = state.atTime,
          setTime = if (state.runningQueryCount != 0) None else Some(setAtTime(_)),
          toggleLayout = toggleNetworkLayout,
          recenterViewport = recenterNetworkViewport
        ),
        downloadSvg = () => downloadSvgSnapshot()
      )
    }

    elements += Loader(
      keyName = "loader",
      pendingCount = state.runningQueryCount,
      onClick = if (props.queryMethod == QueryMethod.WebSocket) Some(() => cancelQueries()) else None
    )

    elements += VisNetwork(
      props.graphData,
      afterNetworkInit,
      onContextMenu = _.preventDefault(),
      onClick = _ => setState(_.copy(contextMenuOpt = None)),
      onKeyDown = networkKeyDown(_),
      options = networkOptions
    )

    // Update whether the graph should be animating
    network.foreach(_.setOptions(new vis.Network.Options {
      override val physics = new vis.Network.Options.Physics {
        override val enabled = state.animating
      }
    }))

    for (messageContent <- state.bottomBar)
      elements += MessageBar(
        message = messageContent,
        closeMessageBox = () => setState(_.copy(bottomBar = None))
      )

    state.contextMenuOpt.foreach { contextMenu =>
      elements += ContextMenu(
        x = contextMenu.x,
        y = contextMenu.y,
        items = contextMenu.items
      )
    }

    div(
      style := jsObj(height = "100%", width = "100%", overflow = "hidden", position = "relative")
    )(elements.result(): _*)
  }
}

sealed abstract class UiQueryType
object UiQueryType {

  /** Query is text, and results should go in the green message bar */
  case object Text extends UiQueryType

  /** Query is for nodes/edges, and results should be spread across the canvas */
  case object Node extends UiQueryType

  /** Query is for nodes/edges, and results should explode out from one node
    *
    * @param explodeFromId from which node should results explode
    * @param syntheticEdgeLabel if set, also draw a purple dotted edge (with this
    *                           label) from the central node to all of the other nodes
    */
  final case class NodeFromId(explodeFromId: String, syntheticEdgeLabel: Option[String]) extends UiQueryType
}

/** How `vis` should structure nodes */
sealed abstract class NetworkLayout
object NetworkLayout {
  case object Graph extends NetworkLayout
  case object Tree extends NetworkLayout
}

/** How should queries be relayed to the backend? */
sealed abstract class QueryMethod
object QueryMethod {
  case object Restful extends QueryMethod
  case object WebSocket extends QueryMethod
}

/** This is what we actually store in the `vis` mutable node set. We have
  * to cast nodes coming out of the network into this before being able to use
  * these fields
  *
  * @param uiNode original node data
  */
trait QueryUiVisNodeExt extends vis.Node {
  val uiNode: UiNode[String]
}

/** This is what we actually store in the `vis` mutable edge set. We have
  * to cast edges coming out of the network into this before being able to use
  * these fields
  *
  * @param uiEdge original edge data
  */
trait QueryUiVisEdgeExt extends vis.Edge {
  val uiEdge: UiEdge[String]
  val isSyntheticEdge: Boolean
}

package com.thatdot.quine.app.routes

import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.matching.Regex

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import io.circe.Json

import com.thatdot.common.logging.Log.{
  LazySafeLogging,
  LogConfig,
  OnlySafeStringInterpolator,
  Safe,
  SafeLoggableInterpolator,
}
import com.thatdot.common.logging.Pretty.PrettyHelper
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.compiler.cypher.CypherProcedures
import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.graph.cypher.quinepattern.{
  OutputTarget,
  QueryContext => QPQueryContext,
  QueryPlanner,
  QuinePatternHelpers,
  RuntimeMode,
}
import com.thatdot.quine.graph.cypher.{
  CypherException,
  Expr => CypherExpr,
  RunningCypherQuery => CypherRunningQuery,
  Type => CypherType,
  Value => CypherValue,
}
import com.thatdot.quine.graph.quinepattern.{LoadQuery, QuinePatternOpsGraph}
import com.thatdot.quine.graph.{CypherOpsGraph, LiteralOpsGraph, NamespaceId, StandingQueryId}
import com.thatdot.quine.language.{ast => Pattern}
import com.thatdot.quine.model._
import com.thatdot.quine.routes._
import com.thatdot.quine.util.Log.implicits._

trait QueryUiCypherApiMethods extends LazySafeLogging {
  import QueryUiCypherApiMethods._
  implicit def graph: LiteralOpsGraph with CypherOpsGraph
  implicit def idProvider: QuineIdProvider
  implicit protected def logConfig: LogConfig

  /** Compute the host of a quine ID */
  def hostIndex(qid: QuineId): Int
  private def guessCypherParameters(params: Map[String, Json]): Map[String, CypherValue] =
    params.map { case (k, v) => k -> CypherExpr.fromQuineValue(QuineValue.fromJson(v)) }

  /** Post-process UI nodes. This serves as a hook for last minute modifications to the nodes sent out to the UI.
    *
    * @param uiNode UI node to modify
    * @return updated UI node
    */
  protected def transformUiNode(uiNode: UiNode[QuineId]): UiNode[QuineId]

  /** Query nodes with a given Cypher query
    *
    * @note this filters out nodes whose IDs are not supported by the provider
    *
    * @param query Cypher query expected to return nodes
    * @param namespace Which namespace to query in.
    * @param atTime possibly historical time to query
    * @return tuple of nodes produced by the query, whether the query is read-only, and whether the query may cause full node scan
    */
  final def queryCypherNodes(
    query: CypherQuery,
    namespace: NamespaceId,
    atTime: Option[Milliseconds],
  ): (Source[UiNode[QuineId], NotUsed], Boolean, Boolean) = {
    // QuinePattern branch - early return to keep original code unchanged below
    if (isQuinePatternEnabled) return quinePatternQueryNodes(query, namespace)

    val res: CypherRunningQuery = cypher.queryCypherValues(
      query.text,
      parameters = guessCypherParameters(query.parameters),
      namespace = namespace,
      atTime = atTime,
    )

    val results = res.results
      .mapConcat(identity) // this function returns all columns from all rows as 1 sequence without any grouping
      .mapConcat[UiNode[QuineId]] {
        case CypherExpr.Node(qid, labels, properties) =>
          val nodeLabel = if (labels.nonEmpty) {
            labels.map(_.name).mkString(":")
          } else {
            "ID: " + qid.pretty
          }

          Some(
            UiNode(
              id = qid,
              hostIndex = hostIndex(qid),
              label = nodeLabel,
              properties = properties.map { case (k, v) => (k.name, CypherValue.toJson(v)) },
            ),
          )

        case CypherExpr.Null =>
          // node-typed values that are null are just ignored rather than generating an error, because they are easily
          // introduced with eg `OPTIONAL MATCH`
          None

        case other =>
          // non-null, non-node values cannot be handled by the pre-UI post-query processing logic, so we need
          // to drop or error on them. Since the usage contract for this functionality is "I have a query that
          // returns nodes", we consider this case as bad user input and return an error.
          throw CypherException.TypeMismatch(
            expected = Seq(CypherType.Node),
            actualValue = other,
            context = "node query return value",
          )
      }
      .map(transformUiNode)

    (results, res.compiled.isReadOnly, res.compiled.canContainAllNodeScan)
  }

  /** Query edges with a given Cypher query
    *
    * @note this filters out nodes whose IDs are not supported by the provider
    *
    * @param query Cypher query expected to return edges
    * @param namespace the namespace in which to run this query
    * @param atTime possibly historical time to query
    * @param requestTimeout timeout signalling output results no longer matter
    * @return tuple of edges produced by the query, readonly, and canContainAllNodeScan
    */
  def queryCypherEdges(
    query: CypherQuery,
    namespace: NamespaceId,
    atTime: Option[Milliseconds],
    requestTimeout: Duration = Duration.Inf,
  ): (Source[UiEdge[QuineId], NotUsed], Boolean, Boolean) = {
    // QuinePattern branch - early return to keep original code unchanged below
    if (isQuinePatternEnabled) return quinePatternQueryEdges(query, namespace)

    val res: CypherRunningQuery = cypher.queryCypherValues(
      query.text,
      parameters = guessCypherParameters(query.parameters),
      namespace = namespace,
      atTime = atTime,
    )

    val results = res.results
      .mapConcat(identity) // this function returns all columns from all rows as 1 sequence without any grouping
      .mapConcat[UiEdge[QuineId]] {
        case CypherExpr.Relationship(src, lbl, _, tgt) =>
          Some(UiEdge(from = src, to = tgt, edgeType = lbl.name))

        case CypherExpr.Null => None // possibly from OPTIONAL MATCH, see comments in [[queryCypherNodes]]

        case other =>
          throw CypherException.TypeMismatch(
            expected = Seq(CypherType.Relationship),
            actualValue = other,
            context = "edge query return value",
          )
      }

    (results, res.compiled.isReadOnly, res.compiled.canContainAllNodeScan)
  }

  /** Query anything with a given cypher query
    *
    * @note queries starting with `EXPLAIN` are intercepted (since they are
    * anyways not valid Cypher) and return one value which represents the
    * execution plan of the query without running the query.
    *
    * @param query Cypher query
    * @param namespace the namespace in which to run this query
    * @param atTime possibly historical time to query
    * @return tuple of:
    *         - columns of the result
    *         - rows of the result as a Source (each row is a sequence of JSON values whose length matches the
    *           length of the columns)
    *         - boolean isReadOnly
    *         - boolean canContainAllNodeScan
    */
  def queryCypherGeneric(
    query: CypherQuery,
    namespace: NamespaceId,
    atTime: Option[Milliseconds],
  ): (Seq[String], Source[Seq[Json], NotUsed], Boolean, Boolean) = {
    // QuinePattern branch - early return to keep original code unchanged below
    if (isQuinePatternEnabled) return quinePatternQueryGeneric(query, namespace)

    query.text match {
      case Explain(toExplain) =>
        val compiledQuery = cypher
          .compile(queryText = toExplain, unfixedParameters = query.parameters.keys.toSeq)
          .query
        val plan = cypher.Plan.fromQuery(
          compiledQuery,
        )
        logger.debug(safe"User requested EXPLAIN of query: $compiledQuery")
        (Vector("plan"), Source.single(Seq(CypherValue.toJson(plan.toValue))), true, false)
      // rewrite "SHOW PROCEDURES" to an equivalent `help.procedures` call, if possible
      case ShowProcedures(rewritten, warning) =>
        warning.foreach(logger.warn(_))
        queryCypherGeneric(CypherQuery(rewritten, query.parameters), namespace, atTime)

      // TODO add support for PROFILE statement

      case queryText =>
        val runnableQuery = cypher.queryCypherValues(
          queryText,
          parameters = guessCypherParameters(query.parameters),
          namespace = namespace,
          atTime = atTime,
        )
        val columns = runnableQuery.columns.map(_.name)
        val bodyRows = runnableQuery.results.map(row => row.map(CypherValue.toJson))
        (columns, bodyRows, runnableQuery.compiled.isReadOnly, runnableQuery.compiled.canContainAllNodeScan)
    }
  }

  /** Shared helper that executes a QuinePattern query and returns the raw context stream plus planned metadata.
    * Each quinePatternQuery* method calls this, then applies its own result-mapping step.
    */
  private def executeQuinePattern(
    query: CypherQuery,
    namespace: NamespaceId,
  ): (Source[QPQueryContext, NotUsed], QueryPlanner.PlannedQuery) = {
    import com.thatdot.quine.language.phases.UpgradeModule._
    requireQuinePatternEnabled()
    val parameters = toQuinePatternParameters(query.parameters)
    val qpGraph: QuinePatternOpsGraph = graph.asInstanceOf[QuinePatternOpsGraph]
    implicit val ec = qpGraph.system.dispatcher

    val parser = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase
    val (symbolState, parseResult) = parser.process(query.text).value.run(LexerState(Nil)).value

    val planned = parseResult match {
      case Some(cypherAst) =>
        QueryPlanner.planWithMetadata(cypherAst, symbolState.symbolTable)
      case None =>
        throw new IllegalArgumentException(
          s"Failed to parse query. QuinePattern does not support this query syntax: ${query.text.take(100)}",
        )
    }

    val promise = Promise[Seq[QPQueryContext]]()
    qpGraph.getLoader ! LoadQuery(
      StandingQueryId.fresh(),
      planned.plan,
      RuntimeMode.Eager,
      parameters,
      namespace,
      OutputTarget.EagerCollector(promise),
      planned.returnColumns,
      planned.outputNameMapping,
    )

    val source = Source
      .futureSource(promise.future.map(results => Source(results)))
      .mapMaterializedValue(_ => NotUsed)

    (source, planned)
  }

  private def quinePatternQueryNodes(
    query: CypherQuery,
    namespace: NamespaceId,
  ): (Source[UiNode[QuineId], NotUsed], Boolean, Boolean) = {
    logger.info(safe"Executing node query using QuinePattern interpreter: ${Safe(query.text.take(100))}")
    val (source, _) = executeQuinePattern(query, namespace)
    val results = source
      .mapConcat { qpCtx =>
        qpCtx.bindings.values.flatMap {
          case Pattern.Value.Node(qid, labels, props) =>
            val cypherProps = props.values.map { case (k, v) =>
              k -> QuinePatternHelpers.patternValueToCypherValue(v)
            }
            val nodeLabel = if (labels.nonEmpty) labels.map(_.name).mkString(":") else "ID: " + qid.pretty
            Some(
              UiNode(qid, hostIndex(qid), nodeLabel, cypherProps.map { case (k, v) => (k.name, CypherValue.toJson(v)) }),
            )
          case Pattern.Value.Null => None
          case _ => None
        }.toList
      }
      .map(transformUiNode)

    (results, false, true)
  }

  private def quinePatternQueryEdges(
    query: CypherQuery,
    namespace: NamespaceId,
  ): (Source[UiEdge[QuineId], NotUsed], Boolean, Boolean) = {
    logger.info(safe"Executing edge query using QuinePattern interpreter: ${Safe(query.text.take(100))}")
    val (source, _) = executeQuinePattern(query, namespace)
    val results = source
      .mapConcat { qpCtx =>
        qpCtx.bindings.values.flatMap { case v =>
          val cypherVal = QuinePatternHelpers.patternValueToCypherValue(v)
          cypherVal match {
            case CypherExpr.Relationship(src, lbl, _, tgt) =>
              Some(UiEdge(from = src, to = tgt, edgeType = lbl.name))
            case _ => None
          }
        }.toList
      }

    (results, false, true)
  }

  private def quinePatternQueryGeneric(
    query: CypherQuery,
    namespace: NamespaceId,
  ): (Seq[String], Source[Seq[Json], NotUsed], Boolean, Boolean) = {
    logger.info(safe"Executing query using QuinePattern interpreter: ${Safe(query.text.take(100))}")
    val (source, planned) = executeQuinePattern(query, namespace)
    val columnNames: Seq[String] = planned.outputNameMapping.values.map(_.name).toSeq
    val rowsSource = source.map { qpCtx =>
      columnNames.map { col =>
        val patternValue = qpCtx.bindings.getOrElse(Symbol(col), Pattern.Value.Null)
        val cypherValue = QuinePatternHelpers.patternValueToCypherValue(patternValue)
        CypherValue.toJson(cypherValue)
      }
    }

    (columnNames, rowsSource, false, true)
  }

  // Helper methods for QuinePattern support
  private def isQuinePatternEnabled: Boolean =
    sys.props.get("qp.enabled").flatMap(_.toBooleanOption).getOrElse(false)

  private def requireQuinePatternEnabled(): Unit =
    if (!isQuinePatternEnabled) {
      throw new IllegalStateException("QuinePattern requires -Dqp.enabled=true to be set")
    }

  private def toQuinePatternParameters(params: Map[String, Json]): Map[Symbol, com.thatdot.quine.language.ast.Value] = {
    import com.thatdot.quine.graph.cypher.quinepattern.CypherAndQuineHelpers.quineValueToPatternValue
    params.map { case (k, v) => Symbol(k) -> quineValueToPatternValue(QuineValue.fromJson(v)) }
  }

}
object QueryUiCypherApiMethods extends LazySafeLogging {
  // EXPLAIN <query> (1 argument: query)
  private val Explain: Regex = raw"(?is)\s*explain\s+(.*)".r
  // SHOW PROCEDURES matcher. Matches return 2 values: a converted query using `help.procedures` and an optional
  // SafeInterpolator with a warning to log back to the user
  private object ShowProcedures {
    private val cypherProceduresInvocation = s"CALL ${CypherProcedures.name}()"

    // see https://regex101.com/r/CwK80x/1
    // SHOW PROCEDURES [executable-by filter] [query suffix] (2 arguments).
    // The first argument is unsupported and used only for warnings.
    // The second is usable in-place on the procedure call.
    private val ShowProceduresStatement = raw"(?is)(?:\h*)show\h+procedures?\h*(executable(?: by \S+)?)?\h*(.*)".r

    def unapply(s: String): Option[(String, Option[OnlySafeStringInterpolator])] = s match {
      case ShowProceduresStatement(ignoredArgs, querySuffix) =>
        val rewritten = s"$cypherProceduresInvocation $querySuffix".trim
        val warning =
          Option(ignoredArgs).filter(_.nonEmpty).map { args =>
            safe"Ignoring unsupported arguments to SHOW PROCEDURES: `${Safe(args)}`"
          }
        Some(rewritten -> warning)
      case _ =>
        None
    }
  }
}

class OSSQueryUiCypherMethods(quineGraph: LiteralOpsGraph with CypherOpsGraph)(implicit
  protected val logConfig: LogConfig,
) extends QueryUiCypherApiMethods() {
  def hostIndex(qid: com.thatdot.common.quineid.QuineId): Int = 0
  override def idProvider: QuineIdProvider = graph.idProvider
  def transformUiNode(uiNode: com.thatdot.quine.routes.UiNode[com.thatdot.common.quineid.QuineId]) = uiNode
  override def graph = quineGraph
}

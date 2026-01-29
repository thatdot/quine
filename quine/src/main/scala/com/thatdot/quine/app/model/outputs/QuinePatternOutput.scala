package com.thatdot.quine.app.model.outputs

import scala.collection.immutable.SortedMap
import scala.concurrent.Promise

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Source}

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig}
import com.thatdot.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.language.{ast => Pattern}
import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.cypher.quinepattern.CypherAndQuineHelpers.quineValueToPatternValue
import com.thatdot.quine.graph.cypher.quinepattern.{
  OutputTarget,
  QueryContext => QPQueryContext,
  QueryPlanner,
  RuntimeMode,
}
import com.thatdot.quine.graph.cypher.{Expr, QueryContext}
import com.thatdot.quine.graph.quinepattern.{LoadQuery, QuinePatternOpsGraph}
import com.thatdot.quine.graph.{CypherOpsGraph, MasterStream, NamespaceId, StandingQueryId, StandingQueryResult}
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.QuinePatternQuery
import com.thatdot.quine.serialization.ProtobufSchemaCache

class QuinePatternOutput(
  config: QuinePatternQuery,
  createRecursiveOutput: (
    String,
    NamespaceId,
    StandingQueryResultOutputUserDef,
    CypherOpsGraph,
    ProtobufSchemaCache,
    LogConfig,
  ) => Flow[StandingQueryResult, SqResultsExecToken, NotUsed],
)(implicit
  private val logConfig: LogConfig,
  private val protobufSchemaCache: ProtobufSchemaCache,
) extends OutputRuntime
    with LazySafeLogging {
  val maybeIsQPEnabled: Option[Boolean] = for {
    pv <- Option(System.getProperty("qp.enabled"))
    b <- pv.toBooleanOption
  } yield b

  maybeIsQPEnabled match {
    case Some(true) => ()
    case _ => sys.error("Quine pattern must be enabled using -Dqp.enabled=true to use this feature.")
  }

  override def flow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  ): Flow[StandingQueryResult, MasterStream.SqResultsExecToken, NotUsed] = {
    val token = execToken(name, inNamespace)

    import com.thatdot.language.phases.UpgradeModule._

    val parser = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase
    val (symbolState, result) = parser.process(config.query).value.run(LexerState(Nil)).value

    val planned = QueryPlanner.planWithMetadata(result.get, symbolState.symbolTable)

    val andThenFlow: Flow[(StandingQueryResult.Meta, QueryContext), SqResultsExecToken, NotUsed] =
      (config.andThen match {
        case Some(thenOutput) =>
          Flow[(StandingQueryResult.Meta, QueryContext)]
            .map { case (meta: StandingQueryResult.Meta, qc: QueryContext) =>
              val newData = qc.environment.map { case (keySym, cypherVal) =>
                keySym.name -> Expr.toQuineValue(cypherVal).getOrElse {
//                  logger.warn(
//                    log"""Cypher Value: ${cypherVal} could not be represented as a Quine value in Standing
//                         |Query output: ${Safe(name)}. Using `null` instead.""".cleanLines,
//                  )
                  QuineValue.Null
                }
              }
              StandingQueryResult(meta, newData)
            }
            .via(createRecursiveOutput(name, inNamespace, thenOutput, graph, protobufSchemaCache, logConfig))
        case None => Flow[(StandingQueryResult.Meta, QueryContext)].map(_ => token)
      }).named(s"sq-output-andthen-for-$name")

    Flow[StandingQueryResult]
      .flatMapMerge(
        breadth = config.parallelism,
        result => {
          val params = Map(
            Symbol("that") -> Pattern.Value.Map(
              SortedMap(
                Symbol("meta") -> Pattern.Value.Map(
                  SortedMap(
                    Symbol("isPositiveMatch") -> (if (result.meta.isPositiveMatch) Pattern.Value.True
                                                  else Pattern.Value.False),
                  ),
                ),
                Symbol("data") -> Pattern.Value.Map(
                  SortedMap.from(result.data.map(p => Symbol(p._1) -> quineValueToPatternValue(p._2))),
                ),
              ),
            ),
          )

          val hack = graph.asInstanceOf[QuinePatternOpsGraph]
          implicit val ec = hack.system.dispatcher

          // Use promise-based EagerCollector
          val promise = Promise[Seq[QPQueryContext]]()
          hack.getLoader ! LoadQuery(
            StandingQueryId.fresh(),
            planned.plan,
            RuntimeMode.Eager,
            params,
            inNamespace,
            OutputTarget.EagerCollector(promise),
            planned.returnColumns,
            planned.outputNameMapping,
            queryName = Some(name),
          )

          Source
            .futureSource(promise.future.map(results => Source(results)))
            .mapMaterializedValue(_ => NotUsed)
            .via(Flow[QPQueryContext].map { qpCtx =>
              // Convert QPQueryContext (pattern values) to QueryContext (cypher values)
              import com.thatdot.quine.graph.cypher.quinepattern.QuinePatternHelpers.patternValueToCypherValue
              val cypherEnv: Map[Symbol, com.thatdot.quine.graph.cypher.Value] =
                qpCtx.bindings.map { case (k, v) => k -> patternValueToCypherValue(v) }
              val qc = QueryContext(cypherEnv)
              StandingQueryResult.Meta(isPositiveMatch = true) -> qc
            })
        },
      )
      .via(andThenFlow)
  }
}

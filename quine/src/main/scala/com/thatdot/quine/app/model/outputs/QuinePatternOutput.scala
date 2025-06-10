package com.thatdot.quine.app.model.outputs

import scala.collection.immutable.SortedMap

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.cypher.quinepattern.CypherAndQuineHelpers.quineValueToPatternValue
import com.thatdot.quine.graph.cypher.quinepattern.QuinePatternHelpers.patternValueToCypherValue
import com.thatdot.quine.graph.cypher.quinepattern.{EagerQuinePatternQueryPlanner, QueryTarget, QuinePatternInterpreter}
import com.thatdot.quine.graph.cypher.{Expr, QueryContext}
import com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph
import com.thatdot.quine.graph.{CypherOpsGraph, MasterStream, NamespaceId, StandingQueryResult}
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.QuinePatternQuery
import com.thatdot.quine.serialization.ProtobufSchemaCache
import com.thatdot.quine.util.Log.implicits._

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
    val (_, result) = parser.process(config.query).value.run(LexerState(Nil)).value
    val queryPlan = EagerQuinePatternQueryPlanner.generatePlan(result.get)

    val andThenFlow: Flow[(StandingQueryResult.Meta, QueryContext), SqResultsExecToken, NotUsed] =
      (config.andThen match {
        case Some(thenOutput) =>
          Flow[(StandingQueryResult.Meta, QueryContext)]
            .map { case (meta: StandingQueryResult.Meta, qc: QueryContext) =>
              val newData = qc.environment.map { case (keySym, cypherVal) =>
                keySym.name -> Expr.toQuineValue(cypherVal).getOrElse {
                  logger.warn(
                    log"""Cypher Value: ${cypherVal} could not be represented as a Quine value in Standing
                         |Query output: ${Safe(name)}. Using `null` instead.""".cleanLines,
                  )
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
          val parameters = Expr.Map(
            SortedMap(
              "meta" -> Expr.Map(SortedMap("isPositiveMatch" -> Expr.Bool(result.meta.isPositiveMatch))),
              "data" -> Expr.Map(
                SortedMap.from(result.data.map(p => p._1 -> patternValueToCypherValue(quineValueToPatternValue(p._2)))),
              ),
            ),
          )
          QuinePatternInterpreter
            .interpret(
              queryPlan,
              QueryTarget.None,
              inNamespace,
              Map(Symbol(config.parameter) -> parameters),
              QueryContext.empty,
              true,
            )(graph.asInstanceOf[QuinePatternOpsGraph])
            .map(qc => (result.meta, qc))
        },
      )
      .via(andThenFlow)
  }
}

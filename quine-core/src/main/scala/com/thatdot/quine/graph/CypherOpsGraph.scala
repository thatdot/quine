package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.Timeout

import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.model._

/** Functionality for querying the graph using Cypher. */
trait CypherOpsGraph extends BaseGraph {

  requireBehavior(classOf[CypherOpsGraph].getSimpleName, classOf[behavior.CypherBehavior])

  /** Maximum expanded length of a variable length pattern.
    *
    * If this is exceeded, the query will failed with an exception.
    */
  val maxCypherExpandVisitedCount = 1000

  /** Default maximum length of a path returned by `shortestPath`.
    *
    * Longer paths will be silently filtered out.
    *
    * @see [[Proc.ShortestPath]]
    */
  val defaultMaxCypherShortestPathLength = 10

  /** Timeout for one step of a Cypher query execution.
    *
    * This does not mean queries must complete within this time, just that a
    * single ask performed as part of the query should complete in this time.
    */
  val cypherQueryProgressTimeout: Timeout = Timeout(30.seconds)

  private def getInterpreter(forTime: Option[Milliseconds]): AnchoredInterpreter =
    new AnchoredInterpreter {
      val graph = CypherOpsGraph.this
      val atTime = forTime
      val idProvider = CypherOpsGraph.this.idProvider
      val cypherEc = CypherOpsGraph.this.shardDispatcherEC
      val cypherProcessTimeout = CypherOpsGraph.this.cypherQueryProgressTimeout
    }

  object cypherOps {

    /* We do a lot of queries on the thoroughgoing present, so cache an instance
     * of an anchored interpreter.
     *
     * TODO: should we cache historical instances too? (eg. with a `LoadingCache`)
     */
    private val currentMomentInterpreter = getInterpreter(None)

    /** Issue a compiled Cypher query for a point in time
      *
      * @param query compiled Cypher query
      * @param parameters constants in the query
      * @param atTime historical moment to query
      * @param context variables already bound going into the query
      * @return rows of results
      */
    def query(
      query: Query[Location.Anywhere],
      parameters: Parameters = Parameters.empty,
      atTime: Option[Milliseconds] = None,
      context: QueryContext = QueryContext.empty
    ): Source[QueryContext, NotUsed] = {
      requiredGraphIsReady()
      val interpreter = if (atTime == None) currentMomentInterpreter else getInterpreter(atTime)
      interpreter
        .interpret(query, context)(parameters)
        .mapMaterializedValue(_ => NotUsed)
    }
  }
}

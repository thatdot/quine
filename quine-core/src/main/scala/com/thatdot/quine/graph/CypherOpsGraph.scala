package com.thatdot.quine.graph
import akka.NotUsed
import akka.stream.scaladsl.Source

import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.model._

/** Functionality for querying the graph using Cypher. */
trait CypherOpsGraph extends BaseGraph {

  requireBehavior("CypherOperations", classOf[behavior.CypherBehavior])

  private def getInterpreter(forTime: Option[Milliseconds]): AnchoredInterpreter =
    new AnchoredInterpreter {
      val graph = CypherOpsGraph.this
      val atTime = forTime
      val idProvider = CypherOpsGraph.this.idProvider
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
        .interpret(query, context)(implicitly, parameters)
        .mapMaterializedValue(_ => NotUsed)
    }
  }
}

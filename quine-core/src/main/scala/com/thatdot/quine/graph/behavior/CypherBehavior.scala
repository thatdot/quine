package com.thatdot.quine.graph.behavior

import scala.concurrent.ExecutionContext

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.cypher.{CompiledQuery, CypherInterpreter, Location, RunningCypherQuery}
import com.thatdot.quine.graph.messaging.CypherMessage.{
  CheckOtherHalfEdge,
  CypherQueryInstruction,
  QueryContextResult,
  QueryPackage
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps}
import com.thatdot.quine.graph.{BaseNodeActor, cypher}

trait CypherBehavior extends cypher.OnNodeInterpreter with BaseNodeActor with QuineIdOps with QuineRefOps {

  /** Run a [[CompiledQuery]] on this node's interpreter
    * NOT THREADSAFE: this closes over and may mutate node state, depending on the [[query]]
    */
  def runQuery(
    query: CompiledQuery[Location.OnNode],
    parameters: Map[String, cypher.Value]
  ): RunningCypherQuery = {
    val nodeInterpreter = this: CypherInterpreter[Location.OnNode]
    query.run(parameters, Map.empty, nodeInterpreter)
  }

  def cypherBehavior(instruction: CypherQueryInstruction): Unit = instruction match {
    case qp @ QueryPackage(query, parameters, qc, _) =>
      qp ?! interpret(query, qc)(parameters)
        .mapMaterializedValue(_ => NotUsed)
        .map(QueryContextResult)
    case ce @ CheckOtherHalfEdge(halfEdge, action, query, parameters, qc, _) =>
      action match {
        // Check for edge
        case None if edges.contains(halfEdge) => receive(ce.queryPackage)
        case None => ce ?! Source.empty
        // Add edge
        case Some(true) =>
          val edgeAdded = processEdgeEvents(EdgeAdded(halfEdge) :: Nil)
          val interpreted = interpret(query, qc)(parameters)
          ce ?! Source
            .futureSource(edgeAdded.map(_ => interpreted)(ExecutionContext.parasitic))
            .map(QueryContextResult)
            .mapMaterializedValue(_ => NotUsed)

        // Remove edge
        case Some(false) =>
          val edgeRemoved = processEdgeEvents(EdgeRemoved(halfEdge) :: Nil)
          val interpreted = interpret(query, qc)(parameters)
          ce ?! Source
            .futureSource(edgeRemoved.map(_ => interpreted)(ExecutionContext.parasitic))
            .map(QueryContextResult)
            .mapMaterializedValue(_ => NotUsed)
      }
  }
}

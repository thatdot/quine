package com.thatdot.quine.graph.behavior

import scala.compat.ExecutionContexts

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.thatdot.quine.graph.NodeChangeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.messaging.CypherMessage.{
  CheckOtherHalfEdge,
  CypherQueryInstruction,
  QueryContextResult,
  QueryPackage
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps}
import com.thatdot.quine.graph.{BaseNodeActor, cypher}

trait CypherBehavior extends cypher.OnNodeInterpreter with BaseNodeActor with QuineIdOps with QuineRefOps {

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
          val edgeAdded = processEvents(EdgeAdded(halfEdge) :: Nil)
          val interpreted = interpret(query, qc)(parameters)
          ce ?! Source
            .futureSource(edgeAdded.map(_ => interpreted)(ExecutionContexts.parasitic))
            .map(QueryContextResult)
            .mapMaterializedValue(_ => NotUsed)

        // Remove edge
        case Some(false) =>
          val edgeRemoved = processEvents(EdgeRemoved(halfEdge) :: Nil)
          val interpreted = interpret(query, qc)(parameters)
          ce ?! Source
            .futureSource(edgeRemoved.map(_ => interpreted)(ExecutionContexts.parasitic))
            .map(QueryContextResult)
            .mapMaterializedValue(_ => NotUsed)
      }
  }
}

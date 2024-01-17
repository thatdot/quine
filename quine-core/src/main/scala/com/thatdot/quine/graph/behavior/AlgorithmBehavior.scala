package com.thatdot.quine.graph.behavior

import scala.concurrent.Future
import scala.util.{Failure, Success}

import org.apache.pekko.stream.scaladsl.Sink

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.cypher.{CompiledQuery, Expr, Location, QueryResults}
import com.thatdot.quine.graph.messaging.AlgorithmMessage._
import com.thatdot.quine.graph.messaging.{AlgorithmCommand, QuineIdAtTime, QuineIdOps, QuineRefOps}
import com.thatdot.quine.graph.{BaseNodeActor, cypher}
import com.thatdot.quine.model.QuineId

trait AlgorithmBehavior extends BaseNodeActor with QuineIdOps with QuineRefOps with LazyLogging {

  /** Dependency: run a cypher query on this node (implemented by [[CypherBehavior.runQuery]]) */
  def runQuery(
    query: CompiledQuery[Location.OnNode],
    parameters: Map[String, cypher.Value]
  ): QueryResults

  protected def algorithmBehavior(command: AlgorithmCommand): Unit = command match {
    case GetRandomWalk(collectQuery, depth, returnParam, inOutParam, seedOpt, replyTo) =>
      this.receive(
        AccumulateRandomWalk(
          collectQuery,
          depth,
          edges.all.map(_.other).toSet,
          returnParam,
          inOutParam,
          seedOpt,
          Nil,
          None,
          Set.empty,
          replyTo
        )
      )

    case m @ AccumulateRandomWalk(
          collectQuery,
          remainingDepth,
          neighborhood,
          returnParam,
          inOutParam,
          seedOpt,
          prependAcc,
          validateHalfEdge,
          excludeOther,
          reportTo
        ) =>
      val edgeIsValidated = validateHalfEdge.fold(true)(he => edges.contains(he))
      var weightSum = 0d
      val weightedEdges = edges.all.collect {
        case e if !excludeOther.contains(e.other) =>
          // Note: negative values of `returnParam` and `inOutParam` are filtered out at the API level.
          val edgeChoiceWeight = {
            if (validateHalfEdge.exists(_.other == e.other)) {
              if (returnParam == 0d) 0d else 1d / returnParam // If zero, never return to previous node.
            } else if (neighborhood.contains(e.other)) { // If `inOutParam=1`, `neighborhood` will be empty (see below).
              if (inOutParam == 0d) 0d else 1d // If zero, never visit neighborhood.
            } else if (inOutParam == 0) 1d // If never visiting neighborhood, weight non-neighbors equally at baseline.
            else 1d / inOutParam // Final case determines how to weight non-neighbors with a non-zero in-out param.
          }
          weightSum += edgeChoiceWeight
          edgeChoiceWeight -> e
      }.toList

      def getCypherWalkValues(query: CompiledQuery[Location.OnNode]): Future[List[String]] =
        runQuery(query, Map("n" -> Expr.Bytes(qid))).results
          .mapConcat { row =>
            row.flatMap {
              case Expr.List(v) => v.toList.map(x => Expr.toQuineValue(x).pretty)
              case value => List(Expr.toQuineValue(value).pretty)
            }
          }
          .runWith(Sink.seq[String])
          .map(_.toList)(graph.nodeDispatcherEC)

      if (!edgeIsValidated) {
        // Half-edge not confirmed. Send it back to try again, excluding this ID
        sender() ! m.copy(excludeOther = excludeOther + qid, validateHalfEdge = None)

      } else if (weightedEdges.size < 1 || remainingDepth <= 0) {
        // No edges available, or end of the line. Add this node and report result.
        getCypherWalkValues(collectQuery).onComplete {
          case Success(strings) =>
            GetRandomWalk(collectQuery, 0, 0d, 0d, None, reportTo) ?!
              RandomWalkResult((strings.reverse ++ prependAcc).reverse, didComplete = true)
          case Failure(e) =>
            logger.error(s"Getting walk values on node: ${qid.pretty} failed: $e")
            GetRandomWalk(collectQuery, 0, 0d, 0d, None, reportTo) ?!
            RandomWalkResult(prependAcc.reverse, didComplete = false)
        }(graph.nodeDispatcherEC)

      } else {
        // At the mid-point: prepend, choose a new edge, decrement remaining, and continue.
        val rand = seedOpt.fold(new scala.util.Random()) { seedString =>
          new util.Random((prependAcc.lastOption.getOrElse(qid), seedString, remainingDepth).hashCode)
        }
        val target = weightSum * rand.nextDouble()
        var randAcc = 0d
        val chosenEdge = weightedEdges
          .find { case (weight, _) =>
            randAcc += weight
            randAcc >= target
          }
          .get // It will never be possible for this `.get` to fail. `randAcc` will never be less than `target`.
          ._2

        // An `inOutParam` value of `1` means that neighbors and non-neighbors should be equally weighted.
        // So don't bother sending the neighborhood (and creating a potentially large set) if `inOutParam == 1`.
        val neighborhood = if (inOutParam == 1d) Set.empty[QuineId] else edges.all.map(_.other).toSet

        val _ = getCypherWalkValues(collectQuery).onComplete {
          case Success(strings) =>
            val msg = AccumulateRandomWalk(
              collectQuery,
              remainingDepth - 1,
              neighborhood,
              returnParam,
              inOutParam,
              seedOpt,
              strings.reverse ++ prependAcc,
              Some(chosenEdge.reflect(qid)),
              Set.empty,
              reportTo
            )
            QuineIdAtTime(chosenEdge.other, atTime) ! msg
          case Failure(e) =>
            // If collecting values fails, conclude/truncate the walk and return the results accumulated so far.
            logger.error(s"Getting walk values on node: ${qid.pretty} failed: $e")
            GetRandomWalk(collectQuery, 0, 0d, 0d, None, reportTo) ?!
            RandomWalkResult(prependAcc.reverse, didComplete = false)
        }(graph.nodeDispatcherEC)
      }
  }
}

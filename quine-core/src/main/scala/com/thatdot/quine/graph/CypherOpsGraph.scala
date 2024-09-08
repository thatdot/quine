package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.{ActorRef, PoisonPill}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalCause, RemovalNotification}
import org.apache.pekko

import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.model._
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

final case class SkipOptimizerKey(
  location: Query[Location.External],
  namespace: NamespaceId,
  atTime: Option[Milliseconds],
)

/** Functionality for querying the graph using Cypher. */
trait CypherOpsGraph extends BaseGraph {
  private[this] def requireCompatibleNodeType(): Unit =
    requireBehavior[CypherOpsGraph, behavior.CypherBehavior]

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

  object cypherOps {

    private val loader: CacheLoader[SkipOptimizerKey, ActorRef] =
      new CacheLoader[SkipOptimizerKey, ActorRef] {
        def load(key: SkipOptimizerKey): ActorRef =
          system.actorOf(
            pekko.actor.Props(new SkipOptimizingActor(CypherOpsGraph.this, key.location, key.namespace, key.atTime)),
          )
      }

    // INV queries used as keys must have no Parameters
    val skipOptimizerCache: LoadingCache[SkipOptimizerKey, ActorRef] =
      CacheBuilder.newBuilder
        .maximumSize(100) // TODO arbitrary
        .removalListener { // NB invoked semi-manually via [[SkipOptimizingActor.decommission]]
          (notification: RemovalNotification[SkipOptimizerKey, ActorRef]) =>
            /** allow REPLACED actors to live on (eg, as happens when calling [[skipOptimizerCache.refresh]].
              * Otherwise, remove the actor from the actor system as soon as it has burnt down its mailbox
              */
            if (notification.getCause != RemovalCause.REPLACED)
              notification.getValue ! PoisonPill
            else
              logger.info(
                log"""SkipOptimizingActor at ${Safe(notification.getValue)} is being replaced in the Cypher
                     |skipOptimizerCache without removing. This is expected in tests, but not in production. Shutdown
                     |protocol will not be initiated on the actor.""".cleanLines,
              )
        }
        .build(loader)

    /* We do a lot of queries on the thoroughgoing present, so cache an instance
     * of an anchored interpreter.
     */
    private def currentMomentInterpreter(namespace: NamespaceId) =
      new ThoroughgoingInterpreter(CypherOpsGraph.this, namespace)

    /** To start a query, use [[cypherOps.query]] or [[CypherBehavior.runQuery()]] instead
      *
      * Continue processing a [sub]query against the graph. This is used for 2 reasons:
      * 1) to go from an OnNode interpreter to a graph-managed interpreter
      * 2) to change between graph-managed interpreters mid-query
      *
      * @param query                  compiled Cypher query
      * @param parameters             constants in the query
      * @param atTime                 historical moment to query
      * @param context                variables already bound going into the query
      * @param bypassSkipOptimization if true and the query+atTime are otherwise eligible for skip optimizations (see
      *                               [[SkipOptimizingActor]]), the query will be run without using any available
      *                               [[SkipOptimizingActor]] for orchestration
      * @return rows of results
      */
    private[graph] def continueQuery(
      query: Query[Location.External],
      parameters: Parameters = Parameters.empty,
      namespace: NamespaceId,
      atTime: Option[Milliseconds] = None,
      context: QueryContext = QueryContext.empty,
      bypassSkipOptimization: Boolean = false,
    ): Source[QueryContext, NotUsed] = {
      requireCompatibleNodeType()
      val interpreter =
        atTime match {
          case Some(_) => new AtTimeInterpreter(CypherOpsGraph.this, namespace, atTime, bypassSkipOptimization)
          case None => currentMomentInterpreter(namespace)
        }

      require(
        interpreter.namespace == namespace,
        "Refusing to execute a query in a different namespace than requested by the caller",
      )

      require(
        interpreter.atTime == atTime,
        "Refusing to execute a query at a different timestamp than requested by the caller",
      )

      interpreter
        .interpret(query, context)(parameters, logConfig)
        .mapMaterializedValue(_ => NotUsed)
        .named(s"cypher-query-atTime-${atTime.fold("none")(_.millis.toString)}")
    }

    /** Issue a query against the graph, allowing the graph to pick an interpreter
      *
      * The query must be a [[Location.Anywhere]] query (i.e., must not depend directly on node-local information).
      * Queries that contain node-entering subqueries (e.g., AnchoredEntry) are allowed.
      *
      * @param query                  compiled Cypher query
      * @param atTime                 historical moment to query
      * @param parameters             constants in the query
      * @param bypassSkipOptimization if true and the query+atTime are otherwise eligible for skip optimizations (see
      *                               [[SkipOptimizingActor]]), the query will be run without using any available
      *                               [[SkipOptimizingActor]] for orchestration
      */
    def query(
      query: CompiledQuery[Location.External],
      namespace: NamespaceId,
      atTime: Option[Milliseconds],
      parameters: Map[String, cypher.Value],
      bypassSkipOptimization: Boolean = false,
    ): RunningCypherQuery = {
      requireCompatibleNodeType()
      val interpreter: CypherInterpreter[Location.External] = atTime match {
        case Some(_) => new AtTimeInterpreter(CypherOpsGraph.this, namespace, atTime, bypassSkipOptimization)
        case None => currentMomentInterpreter(namespace)
      }

      query.run(parameters, Map.empty, interpreter)
    }
  }
}

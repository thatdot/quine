package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt

import akka.NotUsed
import akka.actor.{ActorRef, PoisonPill}
import akka.stream.scaladsl.Source
import akka.util.Timeout

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalCause, RemovalNotification}

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

  object cypherOps {

    // INV queries used as keys must have no Parameters
    val skipOptimizerCache: LoadingCache[(Query[Location.Anywhere], Option[Milliseconds]), ActorRef] =
      CacheBuilder
        .newBuilder()
        .maximumSize(100) // TODO arbitrary
        .removalListener { // NB invoked semi-manually via [[SkipOptimizingActor.decommission]]
          (notification: RemovalNotification[(Query[Location.Anywhere], Option[Milliseconds]), ActorRef]) =>
            /** allow REPLACED actors to live on (eg, as happens when calling [[skipOptimizerCache.refresh]].
              * Otherwise, remove the actor from the actor system as soon as it has burnt down its mailbox
              */
            if (notification.getCause != RemovalCause.REPLACED) {
              notification.getValue ! PoisonPill
            } else {
              logger.info(
                s"""SkipOptimizingActor at ${notification.getValue} is being replaced in the Cypher skipOptimizerCache
                   |without removing. This is expected in tests, but not in production. Shutdown protocol will
                   |not be initiated on the actor.""".stripMargin.replace('\n', ' ')
              )
            }
        }
        .build(new CacheLoader[(Query[Location.Anywhere], Option[Milliseconds]), ActorRef] {
          def load(key: (Query[Location.Anywhere], Option[Milliseconds])): ActorRef =
            system.actorOf(akka.actor.Props(new SkipOptimizingActor(CypherOpsGraph.this, key._1, key._2)))
        })

    /* We do a lot of queries on the thoroughgoing present, so cache an instance
     * of an anchored interpreter.
     */
    private val currentMomentInterpreter = new ThoroughgoingInterpreter(CypherOpsGraph.this)

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
      context: QueryContext = QueryContext.empty,
      bypassSkipOptimization: Boolean = false
    ): Source[QueryContext, NotUsed] = {
      requiredGraphIsReady()
      val interpreter = atTime match {
        case Some(millisTime) => new AtTimeInterpreter(CypherOpsGraph.this, millisTime, bypassSkipOptimization)
        case None => currentMomentInterpreter
      }

      interpreter
        .interpret(query, context)(parameters)
        .mapMaterializedValue(_ => NotUsed)
    }
  }
}

package com.thatdot.quine.graph.cypher

import scala.collection.View

import com.thatdot.quine.graph.behavior.MultipleValuesStandingQueryPartSubscription
import com.thatdot.quine.graph.cypher.MultipleValuesResultsReporter.generateResultReports
import com.thatdot.quine.graph.messaging.StandingQueryMessage.MultipleValuesStandingQuerySubscriber
import com.thatdot.quine.graph.{
  NamespaceId,
  RunningStandingQuery,
  StandingQueryId,
  StandingQueryOpsGraph,
  StandingQueryResult
}
import com.thatdot.quine.model.Properties
import com.thatdot.quine.util.Log.LogConfig

/** This class manages a stateful flatmap operation for SQ results reporting. Effectively, this is a node's
  * representation / proxy to the GlobalSubscriber for a given SQ. Because MVSQ results are reported in groups
  * (i.e. all results for a given part of a given SQ are reported at once), we need a way to track which results
  * have been reported and which haven't. Examining the values of the results themselves is insufficient, because
  * the same result rows can be reported multiple times (e.g. if the same value is reported by multiple sources).
  */
class MultipleValuesResultsReporter(
  val sq: RunningStandingQuery,
  initialResultsSnapshot: Seq[QueryContext]
)(implicit protected val logConfig: LogConfig) {

  /** This can be thought of as a table, with all the same columns as each QueryContext, plus an additional column
    * tracking who reported the result. However, since results always arrive in complete snapshots, we can just
    * keep them grouped. Since this is owned by a since node, we know all the result reports came from that node.
    * Since standing queries always have a unique "last step" (you can never report results for a SQ from different
    * parts of a query -- they always propagate back "up" the query before reporting) the results always come from
    * the same partId. Therefore, as an optimization, we don't actually need to store _anything_ except the table of
    * results.
    */
  private var lastResults: Seq[QueryContext] =
    initialResultsSnapshot

  /** Apply a new result set, emitting any results users haven't seen yet and cancelling results that are no longer
    * valid. Returns true iff all reports were successfully enqueued.
    */
  def applyAndEmitResults(resultsGroup: Seq[QueryContext]): Boolean = {
    val reports =
      generateResultReports(lastResults, resultsGroup, includeCancellations = sq.query.queryPattern.includeCancellation)

    val allReportsSucceeded = reports.map(sq.offerResult).toSeq.forall(identity)

    lastResults = resultsGroup
    allReportsSucceeded
  }
}
object MultipleValuesResultsReporter {

  def generateResultReports(
    trackedResults: Seq[QueryContext],
    newResults: Seq[QueryContext],
    includeCancellations: Boolean
  ): View[StandingQueryResult] = {
    val removedRows = trackedResults.diff(newResults)
    val addedRows = newResults.diff(trackedResults)
    val diff = addedRows.view.map(_ -> true) ++ removedRows.view.map(_ -> false)
    diff.collect {
      case (values, isPositiveMatch)
          // we only need to report the members of the diff that are positive matches, unless the query
          // specifies to include cancellations
          if isPositiveMatch || includeCancellations =>
        StandingQueryResult(
          isPositiveMatch,
          values.environment.map { case (k, v) =>
            k.name -> Expr.toQuineValue(v)
          }
        )
    }
  }

  /** Utility to assist in rehydrating the reporters for a node. This is used while waking a node, and should be called
    * from the node's constructor, as it closes over both the node's properties and its standing query states.
    */
  def rehydrateReportersOnNode(
    statesAndSubscribers: Iterable[(MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState)],
    nodeProperties: Properties,
    graph: StandingQueryOpsGraph,
    namespace: NamespaceId
  )(implicit logConfig: LogConfig): Map[StandingQueryId, MultipleValuesResultsReporter] = {
    val cypherProperties = nodeProperties - graph.labelsProperty
    def containsGlobalSubscriber(subscribers: Iterable[MultipleValuesStandingQuerySubscriber]): Boolean =
      subscribers.exists {
        case _: MultipleValuesStandingQuerySubscriber.GlobalSubscriber => true
        case _ => false
      }

    case class ActiveQueryRootedOnThisNode(
      id: StandingQueryId,
      runningInstance: RunningStandingQuery,
      topLevelState: MultipleValuesStandingQueryState
    )

    val topLevelSqStates: Seq[ActiveQueryRootedOnThisNode] =
      statesAndSubscribers
        .collect {
          // first, select only the MVSQ parts with a global subscriber
          case (MultipleValuesStandingQueryPartSubscription(partId @ _, sqId, subscribers), state)
              if containsGlobalSubscriber(subscribers) =>
            (subscribers.toSeq, sqId, state)
        }
        .flatMap { case (subscribers, sqId, state) =>
          // then, extract those subscribers that are global
          subscribers.collect { case _: MultipleValuesStandingQuerySubscriber.GlobalSubscriber =>
            sqId -> state
          }
        }
        .flatMap { case (sqId, state) =>
          // finally, filter out any SQs that are no longer running on the graph as a whole
          val sqIfDefined: Option[RunningStandingQuery] =
            graph.standingQueries(namespace).flatMap(_.runningStandingQuery(sqId))
          sqIfDefined.map { sq =>
            ActiveQueryRootedOnThisNode(sqId, sq, state)
          }
        }
        .toSeq
    // finally, pull the actual results for each of those SQs
    topLevelSqStates.map { case ActiveQueryRootedOnThisNode(id, sq, state) =>
      id -> new MultipleValuesResultsReporter(sq, state.readResults(cypherProperties).getOrElse(Seq.empty))
    }
  }.toMap
}

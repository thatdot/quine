package com.thatdot.quine.graph

import com.thatdot.quine.graph.StandingQueryPattern.MultipleValuesQueryPattern
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery

/** Index of standing queries within a single namespace. This includes:
  * - A forward index by top level StandingQueryId to the corresponding RunningStandingQuery instance
  * - An inverted index by query part id to the corresponding MultipleValuesStandingQuery AST node that is somewhere
  *   inside the values of the forward index.
  *
  * The inverted index contains each of the parts in the forward index and no additional parts. The forward index
  * also includes domain graph (distinct id) queries and Quine Pattern queries. These have global indexes that must be
  * maintained separately. Future improvements could refactor the inverted indexes for the parts in those to also be
  * maintained in an unavoidably consistent manner.
  *
  * @param queries the running standing queries, keyed by their ID
  * @param partIndex index from Multiple Values Part IDs to their query definitions (derived from queries)
  */
case class NamespaceSqIndex private ( // Private constructor to force updates through consistency-maintaining methods
  queries: Map[StandingQueryId, RunningStandingQuery],
  partIndex: Map[MultipleValuesStandingQueryPartId, MultipleValuesStandingQuery],
) {

  /** Add a standing query to the index.
    *
    * For MultipleValuesQueryPattern queries, this also indexes all subquery parts.
    * If a part ID collision is detected (same ID, different query), the existing
    * part is kept and a warning should be logged by the caller.
    *
    * @param sqId the standing query ID
    * @param runningSq the running standing query
    * @return tuple of (new index, map of any part ID collisions detected)
    */
  def withQuery(
    sqId: StandingQueryId,
    runningSq: RunningStandingQuery,
  ): (
    NamespaceSqIndex,
    Map[MultipleValuesStandingQueryPartId, (MultipleValuesStandingQuery, MultipleValuesStandingQuery)],
  ) = {
    val newQueries = queries + (sqId -> runningSq)

    // Type alias for collision map to help type inference
    type CollisionMap =
      Map[MultipleValuesStandingQueryPartId, (MultipleValuesStandingQuery, MultipleValuesStandingQuery)]

    // Extract and index parts for MultipleValuesQueryPattern
    val (newPartIndex, collisions): (
      Map[MultipleValuesStandingQueryPartId, MultipleValuesStandingQuery],
      CollisionMap,
    ) =
      runningSq.query.queryPattern match {
        case MultipleValuesQueryPattern(compiledQuery, _, _) =>
          val partsToAdd = MultipleValuesStandingQuery.indexableSubqueries(compiledQuery)
          val emptyCollisions: CollisionMap = Map.empty
          partsToAdd.foldLeft((partIndex, emptyCollisions)) { case ((idx, colls), newPart) =>
            val partId = newPart.queryPartId
            idx.get(partId) match {
              case Some(existing) if existing != newPart =>
                // Collision: different query with same part ID
                (idx, colls + (partId -> (existing, newPart)))
              case Some(_) =>
                // Same part already registered, no change needed
                (idx, colls)
              case None =>
                // New part, add to index
                (idx + (partId -> newPart), colls)
            }
          }
        case _ =>
          // Non-MVSQ patterns don't need part indexing
          (
            partIndex,
            Map.empty[MultipleValuesStandingQueryPartId, (MultipleValuesStandingQuery, MultipleValuesStandingQuery)],
          )
      }

    (new NamespaceSqIndex(newQueries, newPartIndex), collisions)
  }

  /** Remove a standing query from the index.
    *
    * This rebuilds the part index from remaining queries, ensuring no stale
    * entries remain. This is the key fix for the NPE bug where cancelled
    * queries left stale entries in the part index.
    *
    * @param sqId the standing query ID to remove
    * @return new index with the query removed, or this index if query wasn't present
    */
  def withoutQuery(sqId: StandingQueryId): NamespaceSqIndex =
    queries.get(sqId) match {
      case None => this
      case Some(_) =>
        val newQueries = queries - sqId
        NamespaceSqIndex(newQueries)
    }

  /** Look up a standing query part by its ID.
    *
    * @param partId the part ID to look up
    * @return the query part, or None if not found
    */
  def getQueryPart(partId: MultipleValuesStandingQueryPartId): Option[MultipleValuesStandingQuery] =
    partIndex.get(partId)
}

object NamespaceSqIndex {

  /** Empty index with no queries */
  val empty: NamespaceSqIndex = new NamespaceSqIndex(Map.empty, Map.empty)

  def apply(queries: Map[StandingQueryId, RunningStandingQuery]): NamespaceSqIndex =
    new NamespaceSqIndex(queries, buildPartIndex(queries.values))

  /** Build the part index from a collection of running queries.
    *
    * This extracts all indexable subqueries from MultipleValuesQueryPattern
    * queries and indexes them by their part ID.
    *
    * @param queries the running queries to index
    * @return map from part ID to query definition
    */
  private def buildPartIndex(
    queries: Iterable[RunningStandingQuery],
  ): Map[MultipleValuesStandingQueryPartId, MultipleValuesStandingQuery] = {
    val mvQueries = queries
      .map(_.query.queryPattern)
      .collect { case MultipleValuesQueryPattern(sq, _, _) => sq }

    val allParts = mvQueries.foldLeft(Set.empty[MultipleValuesStandingQuery]) { (acc, sq) =>
      MultipleValuesStandingQuery.indexableSubqueries(sq, acc)
    }

    // Group by part ID; if multiple parts have the same ID, take the last one
    allParts.groupBy(_.queryPartId).map { case (partId, parts) => partId -> parts.last }
  }
}

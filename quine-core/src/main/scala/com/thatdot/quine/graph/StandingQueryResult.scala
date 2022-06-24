package com.thatdot.quine.graph

import com.thatdot.quine.graph.messaging.StandingQueryMessage.ResultId
import com.thatdot.quine.model.{QuineId, QuineIdProvider, QuineValue}

/** Standing query result or cancellation
  *
  * @param meta metadata about the match result
  * @param data on a positive match, the data that was matched. On a cancellation,
  *                undefined (may be empty or, in the case of DistinctId queries,
  *                the id from the initial positive match)
  */
final case class StandingQueryResult(
  meta: StandingQueryResult.Meta,
  data: Map[String, QuineValue]
) {

  /** Return this result as a single `QuineValue` (use sparingly, this effectively throws away type safety!)
    */
  def toQuineValueMap: QuineValue.Map = QuineValue.Map(
    Map(
      "meta" -> QuineValue(meta.toMap),
      "data" -> QuineValue(data)
    )
  )

  def toJson(implicit idProvider: QuineIdProvider): ujson.Value = ujson.Obj(
    "meta" -> meta.toJson,
    "data" -> QuineValue.toJson(QuineValue.Map(data))
  )
}

object StandingQueryResult {

  /** (SQv4) standing query result
    *
    * @param isPositiveMatch is the result reporting a new match (vs. a cancellation)
    * @param resultId ID associated with the result
    * @param data values returned by the standing query
    */
  def apply(
    isPositiveMatch: Boolean,
    resultId: ResultId,
    data: Map[String, QuineValue]
  ): StandingQueryResult = StandingQueryResult(
    StandingQueryResult.Meta(isPositiveMatch, resultId),
    data
  )

  /** (DGB) standing query result
    *
    * @param isPositiveMatch is the result reporting a new match (vs. a cancellation)
    * @param id ID of the root of the match (also the return value)
    * @param formatAsString format of ID to return
    * @param aliasedAs key under which the ID is returned
    */
  def apply(
    isPositiveMatch: Boolean,
    id: QuineId,
    formatAsString: Boolean,
    aliasedAs: String
  )(implicit idProvider: QuineIdProvider): StandingQueryResult = {
    val idValue =
      if (formatAsString) QuineValue.Str(idProvider.qidToPrettyString(id))
      else idProvider.qidToValue(id)
    StandingQueryResult(
      StandingQueryResult.Meta(isPositiveMatch, ResultId.fromQuineId(id)),
      data = Map(aliasedAs -> idValue)
    )
  }

  /** Metadata associated with a standing query result
    *
    * @param isPositiveMatch If this is a result, true. If this is a cancellation, false. If
    *                        cancellations are disabled for this query, always true.
    * @param resultId An ID that uniquely identifies this result within a set of results
    *                 for this standing query. Not necessarily unique across SQs
    *                 TODO consider adding SQ id or name?
    */
  final case class Meta(isPositiveMatch: Boolean, resultId: ResultId) {
    def toMap: Map[String, QuineValue] = Map(
      "isPositiveMatch" -> QuineValue(isPositiveMatch),
      "resultId" -> QuineValue.Str(resultId.uuid.toString)
    )

    def toJson: ujson.Value = ujson.Obj(
      "isPositiveMatch" -> ujson.Bool(isPositiveMatch),
      "resultId" -> ujson.Str(resultId.uuid.toString)
    )
  }
}

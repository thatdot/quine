package com.thatdot.quine.graph

import scala.jdk.CollectionConverters._

import com.google.common.hash.Hashing.{combineOrdered, combineUnordered}
import com.google.common.hash.{HashCode, Hasher, Hashing}
import io.circe.Json

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

  def toJson(implicit idProvider: QuineIdProvider): Json =
    Json.fromFields(
      Seq(
        ("meta", meta.toJson),
        ("data", Json.fromFields(data.view.map { case (k, v) => (k, QuineValue.toJson(v)) }.toSeq))
      )
    )

  // TODO eliminate duplicated code below and in DomainGraphNode.scala

  private def putOrdered[T](seq: Seq[T], into: Hasher, putElement: T => HashCode): Hasher = {
    val size = seq.size
    into.putInt(size)
    if (size > 0) into.putBytes(combineOrdered(seq.map(putElement).asJava).asBytes)
    into
  }

  private def putUnordered[T](iter: Iterable[T], into: Hasher, putElement: T => HashCode): Hasher = {
    val seq = iter.toList
    val size = seq.size
    into.putInt(size)
    if (size > 0) into.putBytes(combineUnordered(seq.map(putElement).asJava).asBytes)
    into
  }

  // hash function implementing the 128-bit murmur3 algorithm
  private def newHasher = Hashing.murmur3_128.newHasher

  private def putQuineValueMapKeyValue(keyValue: (String, QuineValue), into: Hasher): Hasher = {
    val (key, value) = keyValue
    into.putUnencodedChars(key)
    putQuineValue(value, into)
  }

  //TODO this is a duplicate block with DomainGraphNode#putQuineValue
  private def putQuineValue(from: QuineValue, into: Hasher): Hasher =
    from match {
      case QuineValue.Str(string) =>
        into.putByte(0)
        into.putUnencodedChars(string)
      case QuineValue.Integer(long) =>
        into.putByte(1)
        into.putLong(long)
      case QuineValue.Floating(double) =>
        into.putByte(2)
        into.putDouble(double)
      case QuineValue.True =>
        into.putByte(3)
        into.putBoolean(true)
      case QuineValue.False =>
        into.putByte(4)
        into.putBoolean(false)
      case QuineValue.Null =>
        into.putByte(5)
      case QuineValue.Bytes(bytes) =>
        into.putByte(6)
        into.putBytes(bytes)
      case QuineValue.List(list) =>
        into.putByte(7)
        putOrdered[QuineValue](
          list,
          into,
          putQuineValue(_, newHasher).hash
        )
      case QuineValue.Map(map) =>
        into.putByte(8)
        putUnordered[(String, QuineValue)](
          map,
          into,
          putQuineValueMapKeyValue(_, newHasher).hash
        )
      case QuineValue.DateTime(time) =>
        into.putByte(9)
        into.putLong(time.getEpochSecond)
        into.putInt(time.getNano)
      case QuineValue.Id(id) =>
        into.putByte(10)
        into.putBytes(id.array)
      case QuineValue.Duration(d) =>
        into.putByte(11)
        into.putLong(d.getSeconds)
        into.putInt(d.getNano)
      case QuineValue.Date(d) =>
        into.putByte(12)
        into.putLong(d.toEpochDay)
      case QuineValue.Time(t) =>
        into.putByte(13)
        into.putLong(t.toNanoOfDay)
      case QuineValue.LocalDateTime(ldt) =>
        into.putByte(14)
        into.putLong(ldt.toLocalDate.toEpochDay)
        into.putLong(ldt.toLocalTime.toNanoOfDay)
    }

  def dataHashCode: Long =
    putUnordered[(String, QuineValue)](data, newHasher, putQuineValueMapKeyValue(_, newHasher).hash).hash().asLong()
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

    def toJson: Json = Json.fromFields(
      Seq(
        ("isPositiveMatch", Json.fromBoolean(isPositiveMatch)),
        ("resultId", Json.fromString(resultId.uuid.toString))
      )
    )
  }
}

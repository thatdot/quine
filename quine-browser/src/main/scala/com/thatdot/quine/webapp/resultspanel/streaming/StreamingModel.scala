package com.thatdot.quine.webapp.resultspanel.streaming

import io.circe.{Decoder, Json}

/** One decoded streaming frame — the wire contract between a producer and the panel.
  *
  * Raw and pre-enrichment taps emit a `StandingQueryResult` shape:
  * `{ "meta": { "isPositiveMatch": <bool> }, "data": { ... } }`. A post-enrichment tap with
  * an enrichment query emits the enriched Cypher result instead — a flat object of the
  * returned columns with no `meta`/`data` wrapper. We accept both: the wrapped shape when
  * present, otherwise the whole object as `data` (a positive match, since enriched output
  * carries no retraction metadata).
  */
final case class StandingTapFrame(isPositiveMatch: Boolean, data: Json)
object StandingTapFrame {
  implicit val decoder: Decoder[StandingTapFrame] = Decoder.instance { c =>
    val wrapped = for {
      isPositiveMatch <- c.downField("meta").downField("isPositiveMatch").as[Boolean]
      data <- c.downField("data").as[Json]
    } yield StandingTapFrame(isPositiveMatch, data)
    // Pre/Post taps carry no `meta`/`data` wrapper and need not be objects (a transform may
    // emit an array or scalar), so the fallback keeps the whole value as a positive match.
    wrapped.orElse(c.as[Json].map(StandingTapFrame(isPositiveMatch = true, _)))
  }
}

/** One row in a live buffer. `data` is the frame's payload after decoding — an object whose
  * keys are columns, or any other JSON value, surfaced under a single
  * [[StreamRow.ValueColumn]]. `raw` is the frame exactly as it arrived on the wire: for a
  * raw tap that still carries the `{"meta", "data"}` envelope `data` was unwrapped from, so
  * the JSON view can show wire truth while the table stays column-shaped. The column set
  * can grow mid-stream. A retraction (`isMatch == false`) cancels a prior match and is
  * rendered distinctly. `seq` is a stable key for keyed rendering.
  */
final case class StreamRow(seq: Long, isMatch: Boolean, data: Json, raw: Json) {

  /** Column → value for this row: an object payload by its fields; any non-object value
    * under the single [[StreamRow.ValueColumn]].
    */
  def fields: Map[String, Json] =
    data.asObject.map(_.toMap).getOrElse(Map(StreamRow.ValueColumn -> data))

  /** This row's columns, in payload order (object keys, or the single value column). */
  def columnKeys: Iterable[String] =
    data.asObject.map(_.keys).getOrElse(List(StreamRow.ValueColumn))
}
object StreamRow {

  /** Column name for a frame whose payload is not a JSON object (array/scalar/null). */
  val ValueColumn = "value"
}

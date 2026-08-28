package com.thatdot.quine.webapp.util

import io.circe.Json

/** Shaping shared by the two places a background query can be started — the query bar's
  * run-in-background dialog and the Streams page's create form — so they can't disagree about
  * what an under-specified request means.
  */
object BackgroundQueryRequest {

  /** The `Drop` variant of `QuineDestinationSteps` — a discriminator and nothing else. */
  val DropDestination: Json = Json.obj("type" -> Json.fromString("Drop"))

  /** The destinations in a request body; empty when the field is absent, null, or an empty
    * array — all three of which the schema renderer's array widget can leave behind.
    */
  def destinationsOf(body: Json): List[Json] =
    body.hcursor.downField("destinations").as[List[Json]].getOrElse(Nil)

  /** Default `destinations` to `Drop` when none were configured.
    *
    * The server requires a non-empty list, but making the user say so would be ceremony: a run
    * with no destination still does its work and still reports its row count, and the tap relay
    * sees every row before the destinations do — so `Drop` is exactly "run it, don't write the
    * results anywhere", which is what an empty list means anyway.
    *
    * `deepMerge` recurses into objects only, so the array is replaced wholesale rather than
    * merged element-wise — the default can't fuse with a half-filled entry.
    */
  def withDefaultDestination(body: Json): Json =
    if (destinationsOf(body).nonEmpty) body
    else body.deepMerge(Json.obj("destinations" -> Json.arr(DropDestination)))
}

package com.thatdot.quine.webapp.v2api

/** Relative V2 REST paths shared by the fetch and WebSocket clients, so an API path change
  * has one home instead of drifting string literals (the catalog fetch and the tap sockets
  * both address `standingQueries`).
  */
object V2Paths {

  /** The standing-queries collection for `graph` — no leading slash, ready for [[V2Fetch]] or
    * to suffix onto a WebSocket origin.
    */
  def standingQueries(graph: String): String = s"api/v2/graph/$graph/standingQueries"

  /** The background-query executions collection for `graph`. */
  def backgroundQueries(graph: String): String = s"api/v2/graph/$graph/backgroundQueries"

  /** One background-query execution's record. */
  def backgroundQuery(graph: String, executionId: String): String =
    s"${backgroundQueries(graph)}/$executionId"

  /** Cancel one background-query execution (AIP-136 custom verb). */
  def cancelBackgroundQuery(graph: String, executionId: String): String =
    s"${backgroundQuery(graph, executionId)}:cancel"

  /** The result-stream WebSocket for one background-query execution.
    *
    * Hand-built rather than discovered from the OpenAPI spec: the `:tap` endpoints are added
    * to the served routes only and never to `V2ApiInfo.endpointSequences`, so they do not
    * appear in the published document. Same reason the standing-query tap URLs are built by
    * hand in `dataservice.WiretapStore`.
    */
  def backgroundQueryTap(graph: String, executionId: String): String =
    s"${backgroundQuery(graph, executionId)}:tap"

  /** Scheduled jobs. Cluster-wide, not graph-scoped: the target graph lives in a job's action,
    * not in its URL (`V2JobEndpoints` builds these with `rawEndpoint`, not `graphScopedEndpoint`).
    */
  val jobs: String = "api/v2/system/jobs"
}

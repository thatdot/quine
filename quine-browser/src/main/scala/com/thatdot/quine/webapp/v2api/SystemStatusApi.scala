package com.thatdot.quine.webapp.v2api

import scala.concurrent.{ExecutionContext, Future}

import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2ServiceStatus

/** Browser-side reads of the V2 system status endpoint.
  *
  * Lives alongside the [[V2ApiTypes]] model it decodes, in the shared `v2api` package consumed
  * across the landing, streams, and dashboard pages.
  */
object SystemStatusApi {

  /** Best-effort fetch of `GET /api/v2/system/status`, decoded via [[V2ServiceStatus]].
    * `None` when the endpoint is absent (single node) or on any failure.
    *
    * @param baseUrlOpt base URL prefix, resolved with [[V2Fetch]]'s reverse-proxy-aware semantics
    */
  def fetch(baseUrlOpt: Option[String])(implicit ec: ExecutionContext): Future[Option[V2ServiceStatus]] =
    V2Fetch[V2ServiceStatus]("api/v2/system/status", baseUrlOpt)
      .map(Option(_))
      .recover { case _ => None }

  /** Best-effort fetch of the sorted cluster member positions. Empty when the endpoint is absent or on any failure. */
  def memberIndices(baseUrlOpt: Option[String])(implicit ec: ExecutionContext): Future[Seq[Int]] =
    fetch(baseUrlOpt).map(_.map(_.cluster.memberIndices).getOrElse(Seq.empty))
}

package com.thatdot.quine.app.v2api

import scala.concurrent.Future

import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{EndpointInput, query}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.{
  V2AdministrationEndpoints,
  V2AlgorithmEndpoints,
  V2CypherEndpoints,
  V2DebugEndpoints,
  V2IngestEndpoints,
  V2StandingEndpoints,
}

/** Gathering of Quine OSS tapir-defined routes.
  */
class V2OssRoutes(val appMethods: OssApiMethods)(implicit protected val logConfig: LogConfig)
    extends TapirRoutes
    with V2AdministrationEndpoints
    with V2StandingEndpoints
    with V2CypherEndpoints
    with V2AlgorithmEndpoints
    with V2DebugEndpoints
    with V2IngestEndpoints {

  override val apiEndpoints: List[ServerEndpoint[Any, Future]] =
    adminEndpoints ++ standingQueryEndpoints ++ cypherEndpoints ++ algorithmEndpoints ++ debugEndpoints ++ ingestEndpoints

  /** List of endpoints that should not appear in api docs. */
  override val hiddenEndpoints: Set[ServerEndpoint[Any, Future]] = adminHiddenEndpoints.toSet
  //Hidden in OSS api
  def memberIdxParameter: EndpointInput[Option[Int]] =
    query[Option[Int]]("memberIdx").schema(_.hidden(true))
  //Hidden in OSS api
  def namespaceParameter: EndpointInput[Option[String]] =
    query[Option[String]]("namespace").schema(_.hidden(true))
}

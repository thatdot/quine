package com.thatdot.quine.app.v2api

import scala.concurrent.Future

import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{EndpointInput, query}

import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.{V2AdministrationEndpoints, V2StandingEndpoints}

/** Gathering of Quine OSS tapir-defined routes.
  */
class V2OssRoutes(val app: OssApiInterface)
    extends TapirRoutes
    with V2AdministrationEndpoints
    with V2StandingEndpoints {

  override val apiEndpoints: List[ServerEndpoint[Any, Future]] = adminEndpoints ++ standingQueryEndpoints

  /** List of endpoints that should not appear in api docs. */
  override val hiddenEndpoints: Set[ServerEndpoint[Any, Future]] = adminHiddenEndpoints.toSet
  //Hidden in OSS api
  def memberIdxParameter: EndpointInput[Option[Int]] =
    query[Option[Int]]("memberIdx").schema(_.hidden(true))
  //Hidden in OSS api
  def namespaceParameter: EndpointInput[Option[String]] =
    query[Option[String]]("namespace").schema(_.hidden(true))
}

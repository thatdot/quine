package com.thatdot.quine.app.v2api

import scala.concurrent.Future

import sttp.apispec.openapi.Info
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{EndpointInput, query}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.v2api.definitions._

/** Gathering of Quine OSS tapir-defined routes.
  */
class V2OssRoutes(val appMethods: OssApiMethods)(implicit protected val logConfig: LogConfig)
    extends TapirRoutes
    with V2OssEndpointProvider {

  override val apiEndpoints: List[ServerEndpoint[Any, Future]] = V2ApiInfo.endpointSequences(this)

  def memberIdxParameter: EndpointInput[Option[Int]] =
    query[Option[Int]]("memberIdx").schema(_.hidden(true))
  def namespaceParameter: EndpointInput[Option[String]] =
    query[Option[String]]("namespace").schema(_.hidden(true))

  val apiInfo: Info = V2ApiInfo.info

}

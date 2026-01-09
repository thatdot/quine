package com.thatdot.quine.app.v2api

import scala.annotation.nowarn

import sttp.apispec.openapi.OpenAPI
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.{EndpointInput, query}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.v2api.endpoints.Visibility

trait QuineOssV2OpenApiDocs extends V2OssEndpointProvider {

  implicit protected val logConfig: LogConfig

  /** Throws if accessed; only `idProvider` is needed for schema generation. */
  @nowarn("msg=dead code")
  override lazy val appMethods: com.thatdot.quine.app.v2api.definitions.QuineApiMethods =
    throw new NotImplementedError(
      "appMethods should not be accessed during OpenAPI docs generation - " +
      "if you see this error, an endpoint definition is accessing appMethods directly",
    )

  def memberIdxParameter: EndpointInput[Option[Int]] =
    query[Option[Int]]("memberIdx").schema(_.hidden(true))
  def namespaceParameter: EndpointInput[Option[String]] =
    query[Option[String]]("namespace").schema(_.hidden(true))

  private lazy val allRawEndpoints = V2ApiInfo.endpointSequences(this)

  lazy val hiddenPaths: Set[String] = {
    val hiddenEndpoints = allRawEndpoints
      .filter(_.attribute(Visibility.attributeKey).contains(Visibility.Hidden))
      .map(_.endpoint)

    if (hiddenEndpoints.isEmpty) Set.empty
    else {
      val hiddenApi = OpenAPIDocsInterpreter().toOpenAPI(hiddenEndpoints, sttp.apispec.openapi.Info("", ""))
      hiddenApi.paths.pathItems.keys.toSet
    }
  }

  val api: OpenAPI = {
    val visibleEndpoints = allRawEndpoints
      .filterNot(_.attribute(Visibility.attributeKey).contains(Visibility.Hidden))
      .map(_.endpoint)

    OpenAPIDocsInterpreter().toOpenAPI(visibleEndpoints, V2ApiInfo.info)
  }
}

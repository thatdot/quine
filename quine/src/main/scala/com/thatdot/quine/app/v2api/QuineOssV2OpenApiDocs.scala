package com.thatdot.quine.app.v2api

import scala.annotation.nowarn

import sttp.apispec.openapi.{OpenAPI, Server}
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.{AnyEndpoint, EndpointInput, header}

import com.thatdot.api.v2.ProductVersion
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.v2api.definitions.{CommonParameters, CustomMethod, V2OpenApiPostProcessing}
import com.thatdot.quine.app.v2api.endpoints.Visibility
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.routes.exts.NamespaceParameter

trait QuineOssV2OpenApiDocs extends V2OssEndpointProvider {

  implicit protected val logConfig: LogConfig

  /** Throws if accessed; only `idProvider` is needed for schema generation. */
  @nowarn("msg=dead code")
  override lazy val appMethods: com.thatdot.quine.app.v2api.definitions.QuineApiMethods =
    throw new NotImplementedError(
      "appMethods should not be accessed during OpenAPI docs generation - " +
      "if you see this error, an endpoint definition is accessing appMethods directly",
    )

  /** Stated here, since `appMethods` is unavailable during docs generation. */
  override protected def productVersion: ProductVersion = ProductVersion.Oss

  def memberIdxParameter: EndpointInput[Option[Int]] =
    header[Option[Int]]("Quine-Member-Idx").schema(_.hidden(true))
  def namespaceParameter: EndpointInput[Option[NamespaceParameter]] =
    CommonParameters.hiddenValidatingNamespaceQuery

  def graphPrefix: EndpointInput[NamespaceId] = CommonParameters.defaultGraphPrefix

  private lazy val allRawEndpoints = V2ApiInfo.endpointSequences(this)

  lazy val hiddenPaths: Set[String] = {
    val hiddenEndpoints = allRawEndpoints
      .filter(_.attribute(Visibility.attributeKey).contains(Visibility.Hidden))
      .map(_.endpoint)

    if (hiddenEndpoints.isEmpty) Set.empty
    else {
      val hiddenApi = OpenAPIDocsInterpreter().toOpenAPI(hiddenEndpoints, sttp.apispec.openapi.Info("", ""))
      CustomMethod.rewriteOpenAPI(hiddenApi).paths.pathItems.keys.toSet
    }
  }

  /** The endpoints this document describes. */
  lazy val documentedEndpoints: List[AnyEndpoint] = allRawEndpoints
    .filterNot(_.attribute(Visibility.attributeKey).contains(Visibility.Hidden))
    .map(_.endpoint)

  val api: OpenAPI = {
    val raw = OpenAPIDocsInterpreter()
      .toOpenAPI(documentedEndpoints, V2ApiInfo.info)
      .copy(tags = V2ApiInfo.globalTags, servers = List(Server("/")))

    V2OpenApiPostProcessing(raw, productVersion)
  }
}

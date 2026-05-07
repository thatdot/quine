package com.thatdot.quine.app.v2api.definitions

import sttp.tapir.{DecodeResult, Endpoint, EndpointInput, infallibleEndpoint, query, stringToPath}

import com.thatdot.quine.graph.{NamespaceId, defaultNamespaceId}
import com.thatdot.quine.routes.exts.NamespaceParameter

trait CommonParameters {

  def namespaceParameter: EndpointInput[Option[NamespaceParameter]]

  def memberIdxParameter: EndpointInput[Option[Int]]

  def namespaceFromParam(ns: Option[NamespaceParameter]): NamespaceId =
    ns.fold(defaultNamespaceId)(p => NamespaceId(p.namespaceId))
}

/** Concrete deployments provide [[graphPrefix]] — `graph/quine` for OSS, `graph/{graphName}`
  * for Enterprise. The decoded [[NamespaceId]] is the first element of every endpoint's input.
  */
trait GraphScopedEndpoints {

  def graphPrefix: EndpointInput[NamespaceId]

  /** `restPaths` is appended verbatim, including any AIP-136 `:verb` suffix on the final segment. */
  def graphScopedEndpoint(restPaths: String*): Endpoint[Unit, NamespaceId, Nothing, Unit, Any] = {
    val withGraph: EndpointInput[NamespaceId] = (stringToPath("api") / "v2") / graphPrefix
    val full: EndpointInput[NamespaceId] = restPaths.foldLeft(withGraph)((p, seg) => p / seg)
    infallibleEndpoint.in(full)
  }
}

object CommonParameters {

  /** Fixed prefix `graph/quine` for deployments that only target the default namespace
    * (OSS, plus Novelty's hidden internal cypher endpoints).
    */
  val defaultGraphPrefix: EndpointInput[NamespaceId] =
    (stringToPath("graph") / stringToPath("quine")).map(_ => defaultNamespaceId)(_ => ())

  private def decodeNamespace(raw: Option[String]): DecodeResult[Option[NamespaceParameter]] = raw match {
    case None => DecodeResult.Value(None)
    case Some(s) =>
      NamespaceParameter(s) match {
        case Some(p) => DecodeResult.Value(Some(p))
        case None =>
          DecodeResult.Error(s, new IllegalArgumentException(NamespaceParameter.invalidNamespaceMessage(s)))
      }
  }

  private def encodeNamespace(ns: Option[NamespaceParameter]): Option[String] = ns.map(_.namespaceId)

  val validatingNamespaceQuery: EndpointInput[Option[NamespaceParameter]] =
    query[Option[String]]("graphName")
      .mapDecode(decodeNamespace)(encodeNamespace)

  /** Like [[validatingNamespaceQuery]] but hidden from the OpenAPI schema. */
  val hiddenValidatingNamespaceQuery: EndpointInput[Option[NamespaceParameter]] =
    query[Option[String]]("graphName")
      .schema(_.hidden(true))
      .mapDecode(decodeNamespace)(encodeNamespace)
}

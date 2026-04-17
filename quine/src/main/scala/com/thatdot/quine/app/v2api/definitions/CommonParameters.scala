package com.thatdot.quine.app.v2api.definitions

import sttp.tapir.{DecodeResult, EndpointInput, query}

import com.thatdot.quine.graph.{NamespaceId, defaultNamespaceId, namespaceFromString}
import com.thatdot.quine.routes.exts.NamespaceParameter

trait CommonParameters {

  /** Namespace query parameter with validation. Accepts an optional namespace string,
    * validates it against the canonical rules (1-16 chars, letter-start, alphanumeric),
    * lowercases it, and returns a [[NamespaceParameter]]. Invalid values produce a 400.
    */
  def namespaceParameter: EndpointInput[Option[NamespaceParameter]]

  def memberIdxParameter: EndpointInput[Option[Int]]

  /** Convert a validated namespace parameter to a [[NamespaceId]].
    *
    * Delegates to [[namespaceFromString]] which lowercases the input and maps `"default"` to
    * the default namespace (`None`). When no namespace is supplied, returns the default.
    */
  def namespaceFromParam(ns: Option[NamespaceParameter]): NamespaceId =
    ns.fold(defaultNamespaceId)(p => namespaceFromString(p.namespaceId))
}

object CommonParameters {

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

  /** Shared validating namespace query parameter. Parses the `namespace` query string,
    * lowercases and validates via [[NamespaceParameter.apply]], and produces a 400 on failure.
    */
  val validatingNamespaceQuery: EndpointInput[Option[NamespaceParameter]] =
    query[Option[String]]("namespace")
      .mapDecode(decodeNamespace)(encodeNamespace)

  /** Same as [[validatingNamespaceQuery]] but hidden from the OpenAPI schema. */
  val hiddenValidatingNamespaceQuery: EndpointInput[Option[NamespaceParameter]] =
    query[Option[String]]("namespace")
      .schema(_.hidden(true))
      .mapDecode(decodeNamespace)(encodeNamespace)
}

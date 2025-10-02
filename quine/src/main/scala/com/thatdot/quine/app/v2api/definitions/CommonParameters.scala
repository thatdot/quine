package com.thatdot.quine.app.v2api.definitions

import sttp.tapir.EndpointInput

import com.thatdot.quine.graph.NamespaceId

trait CommonParameters {

  /** OSS Specific behavior defined in [[com.thatdot.quine.app.v2api.V2OssRoutes]]. */
  def namespaceParameter: EndpointInput[Option[String]]

  def memberIdxParameter: EndpointInput[Option[Int]]

  //TODO port logic from QuineEndpoints NamespaceParameter
  def namespaceFromParam(ns: Option[String]): NamespaceId =
    ns.flatMap(t => Option.when(t != "default")(Symbol(t)))
}

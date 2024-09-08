package com.thatdot.quine.app

import com.thatdot.quine.graph.{NamespaceId, namespaceToString}

case class NamespaceNotFoundException(namespace: String)
    extends NoSuchElementException(s"Namespace $namespace not found")

object NamespaceNotFoundException {
  def apply(namespaceId: NamespaceId): NamespaceNotFoundException = NamespaceNotFoundException(
    namespaceToString(namespaceId),
  )
}

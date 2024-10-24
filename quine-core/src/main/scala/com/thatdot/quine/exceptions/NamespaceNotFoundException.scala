package com.thatdot.quine.exceptions

import com.thatdot.quine.graph.{NamespaceId, namespaceToString}
import com.thatdot.quine.util.QuineError

case class NamespaceNotFoundException(namespace: String)
    extends NoSuchElementException(s"Namespace $namespace not found")
    with QuineError

object NamespaceNotFoundException {
  def apply(namespaceId: NamespaceId): NamespaceNotFoundException = NamespaceNotFoundException(
    namespaceToString(namespaceId),
  )
}

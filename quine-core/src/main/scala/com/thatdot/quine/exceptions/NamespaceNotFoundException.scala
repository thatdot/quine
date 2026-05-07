package com.thatdot.quine.exceptions

import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.util.QuineError

case class NamespaceNotFoundException(namespace: String)
    extends NoSuchElementException(s"Namespace $namespace not found")
    with QuineError

object NamespaceNotFoundException {
  def fromNamespaceId(namespaceId: NamespaceId): NamespaceNotFoundException =
    NamespaceNotFoundException(namespaceId.name)
}

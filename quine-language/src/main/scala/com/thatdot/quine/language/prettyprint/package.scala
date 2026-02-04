package com.thatdot.quine.language

package object prettyprint extends ResultInstances {
  implicit class PrettyPrintOps[A](private val a: A) extends AnyVal {
    def pretty(implicit pp: PrettyPrint[A]): String = pp.pretty(a)
    def prettyDoc(implicit pp: PrettyPrint[A]): Doc = pp.doc(a)
    def prettyWith(indentWidth: Int)(implicit pp: PrettyPrint[A]): String =
      pp.pretty(a, indentWidth)
  }
}

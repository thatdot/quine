package com.thatdot.quine.util

/** Typeclass instances related to types that are not defined by Quine.
  */
case object TypeclassInstances {

  /** Ordering for arrays of bytes, as used by [[com.thatdot.quine.graph.cypher.Expr.Bytes]] and [[com.thatdot.quine.model.QuineId]]
    * Shorter arrays are "less than" longer arrays, then arrays are compared bytewise.
    */
  implicit object ByteArrOrdering extends Ordering[Array[Byte]] {
    override def compare(lhsArr: Array[Byte], rhsArr: Array[Byte]): Int = {
      val lhsLen: Int = lhsArr.length
      val rhsLen: Int = rhsArr.length

      // Compare first on length, then byte by byte
      if (lhsLen == rhsLen) {
        var i = 0
        var comp = 0
        // find the first non-equal element (if exists)
        while (i < lhsLen && comp == 0) {
          comp = java.lang.Byte.compare(lhsArr(i), rhsArr(i))
          i += 1
        }
        comp
      } else if (lhsLen < rhsLen)
        -1
      else
        1 // (lhsLen > rhsLen)
    }
  }
}

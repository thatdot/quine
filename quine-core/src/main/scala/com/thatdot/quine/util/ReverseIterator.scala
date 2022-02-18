package com.thatdot.quine.util

import scala.collection.mutable.LinkedHashSet
import scala.collection.{AbstractIterator, Iterator}

private[util] class ReverseIterator[A](lastEntry: LinkedHashSet[A]#Entry) extends AbstractIterator[A] {
  private[this] var cur = lastEntry
  def hasNext: Boolean = cur ne null

  def next(): A =
    if (hasNext) { val res = cur.key; cur = cur.earlier; res }
    else Iterator.empty.next()
}

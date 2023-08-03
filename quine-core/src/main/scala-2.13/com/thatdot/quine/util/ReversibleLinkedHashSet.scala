package com.thatdot.quine.util

import annotation.nowarn
import scala.collection.generic.DefaultSerializable
import scala.collection.{Iterator, IterableFactory, IterableFactoryDefaults, StrictOptimizedIterableOps}
import scala.collection.mutable.{LinkedHashSet, SetOps, Growable, GrowableBuilder}

/** Subclass LinkedHashSet to be able to iterate in reverse order.
  * Subclassing is necessary, as `lastEntry` is marked protected.
  * @tparam A
  */
@nowarn // LinkedHashSet was re-implemented in 2.13.11, and also extending it was marked deprecated at the same time
class ReversibleLinkedHashSet[A]
    extends LinkedHashSet[A]
    with SetOps[A, ReversibleLinkedHashSet, ReversibleLinkedHashSet[A]]
    with StrictOptimizedIterableOps[A, ReversibleLinkedHashSet, ReversibleLinkedHashSet[A]]
    with IterableFactoryDefaults[A, ReversibleLinkedHashSet] {

  override def iterableFactory: IterableFactory[ReversibleLinkedHashSet] = ReversibleLinkedHashSet

  def reverseIterator: Iterator[A] = new ReverseIterator[A](lastEntry)

}
object ReversibleLinkedHashSet extends IterableFactory[ReversibleLinkedHashSet] {

  override def empty[A]: ReversibleLinkedHashSet[A] = new ReversibleLinkedHashSet[A]

  def from[E](it: collection.IterableOnce[E]) =
    it match {
      case rlhs: ReversibleLinkedHashSet[E] => rlhs
      case _ => Growable.from(empty[E], it)
    }

  def newBuilder[A] = new GrowableBuilder(empty[A])

}

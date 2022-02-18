package com.thatdot.quine.util

import scala.collection.generic.{CanBuildFrom, GenericCompanion, GenericSetTemplate, MutableSetFactory}
import scala.collection.{Iterator, mutable}

/** Subclass LinkedHashSet to be able to iterate in reverse order.
  * Subclassing is necessary, as `lastEntry` is marked protected.
  * @tparam A
  */
class ReversibleLinkedHashSet[A]
    extends mutable.LinkedHashSet[A]
    with GenericSetTemplate[A, ReversibleLinkedHashSet]
    with mutable.SetLike[A, ReversibleLinkedHashSet[A]] {

  override def companion: GenericCompanion[ReversibleLinkedHashSet] = ReversibleLinkedHashSet

  def reverseIterator: Iterator[A] = new ReverseIterator[A](lastEntry)

}
object ReversibleLinkedHashSet extends MutableSetFactory[ReversibleLinkedHashSet] {
  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, ReversibleLinkedHashSet[A]] =
    ReusableCBF.asInstanceOf[CanBuildFrom[Coll, A, ReversibleLinkedHashSet[A]]]

  private[this] val ReusableCBF = setCanBuildFrom[Any]

  override def empty[A]: ReversibleLinkedHashSet[A] = new ReversibleLinkedHashSet[A]

}

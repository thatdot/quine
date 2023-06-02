package com.thatdot.quine.graph

import scala.annotation.nowarn
import scala.collection.GenTraversable

import org.scalactic.Equality
import org.scalatest.enablers.Sequencing

trait ScalaTestInstances {

  /** Wrapper class to add extension method `.contramap` to {{{Sequencing}}} instances
    * @param sequencingInstance
    * @tparam A
    */
  implicit final private class SequencingMethods[A](val sequencingInstance: Sequencing[A]) {

    /** Use an existing {{{Sequencing}}} method for a new type by mapping over the input
      * @param f A function to translate the new type to a type with an existing {{{Sequencing}}} instance
      * @tparam B The new type
      * @return The {{{Sequencing}}} instance for the new type
      */
    def contramap[B](f: B => A): Sequencing[B] = new Sequencing[B] {
      def containsInOrder(sequence: B, eles: scala.collection.Seq[Any]): Boolean =
        sequencingInstance.containsInOrder(f(sequence), eles)

      def containsInOrderOnly(sequence: B, eles: scala.collection.Seq[Any]): Boolean =
        sequencingInstance.containsInOrderOnly(f(sequence), eles)

      @nowarn("cat=deprecation") // GenTraversable is deprecated in Scala 2.13, but Scalatest requires it here
      def containsTheSameElementsInOrderAs(leftSequence: B, rightSequence: GenTraversable[Any]): Boolean =
        sequencingInstance.containsTheSameElementsInOrderAs(f(leftSequence), rightSequence)
    }
  }

  // Why isn't this already part of ScalaTest?
  implicit def iterableSequencing[E: Equality]: Sequencing[Iterable[E]] =
    Sequencing.sequencingNatureOfGenSeq.contramap(_.toSeq)

}

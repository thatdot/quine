package com.thatdot.quine.compiler.cypher

import cats.implicits._

/** Container for tracking elements with free variables, which makes it possible
  * to efficiently determine which elements transition to being closed as more
  * variables get bound
  *
  * @param variables set of variables in scope
  * @param openAlong elements which have free variables, indexed along a free variable
  */
final case class WithFreeVariables[V, A] private (
  private val variables: Set[V],
  private val openAlong: Map[V, List[(List[V], A)]],
) {
  /* Invariant (not in Scaladoc because it is about private fields):
   *
   * {{{
   * // Keys in `openAlong` are all free variables
   * (variables intersect openAlong.keySet).isEmpty
   * }}}
   *
   *
   *   - if the key is bound first, but there are still other variables that
   *     are still free in the list, we can re-insert the element into the
   *     map under one of those other variables
   *
   *   - if the key is bound last
   */

  def isEmpty: Boolean = openAlong.isEmpty

  /** Bind a variable, so that it is no longer considered "free" in any element
    *
    * @param variable variable that gets bound
    * @return updated context
    */
  def bindVariable(variable: V): (List[A], WithFreeVariables[V, A]) =
    openAlong.get(variable) match {
      case None => (Nil, copy(variables = variables + variable)) // shortcut - nothing changed
      case Some(advancedElems) =>
        /* The values in `openAlong` are tuples of elements with variables that
         * were free in the element when the element was inserted into the map.
         * Since that list is not updated when we bind those variables, it has
         * to be filtered down using `variables`.
         *
         * This lets us split elements into those that still have at least one
         * free-variable and those that are closed.
         */
        val (stillOpen, newlyClosed: List[A]) = advancedElems.partitionEither { case (otherFreeVars, elem) =>
          val newFreeVars: List[V] = otherFreeVars.filter(fv => !variables.contains(fv))
          Either.cond(newFreeVars.isEmpty, elem, newFreeVars -> elem)
        }

        /* Finally, we must construct the next version of `openAlong` by
         * re-inserting elements that still have at least one free-variable.
         * We do this by picking a key free-variable from the list (the head is
         * convenient and fast) then inserting the other free-variables and
         * element as the value.
         */
        val newOpenAlong = stillOpen.foldLeft(openAlong - variable) { case (accOpen, (fvs, elem)) =>
          val newValue = (fvs.tail -> elem) :: accOpen.getOrElse(fvs.head, Nil)
          accOpen + (fvs.head -> newValue)
        }
        newlyClosed -> WithFreeVariables(variables + variable, newOpenAlong)
    }
}
object WithFreeVariables {
  def empty[V, A] = new WithFreeVariables[V, A](Set.empty, Map.empty)

  /** Create a new container
    *
    * @param elems things to put in it
    * @param alreadyInScope variables already in scope (so assume these are all bound)
    * @param getFreeVars given an element, find its free variables
    * @return list for elements without free variables and a container for the others
    */
  def apply[V, A](
    elems: Seq[A],
    alreadyInScope: V => Boolean,
    getFreeVars: A => Set[V],
  ): (List[A], WithFreeVariables[V, A]) = {

    val closed = List.newBuilder[A]
    var openAlong = Map.empty[V, List[(List[V], A)]]

    for (a <- elems) {
      val fvs = getFreeVars(a).filter(v => !alreadyInScope(v))

      // Element if closed if it has no free variablest already in scope
      if (fvs.isEmpty) {
        closed += a
      } else {
        val newValue = (fvs.tail.toList -> a) :: openAlong.getOrElse(fvs.head, Nil)
        openAlong += fvs.head -> newValue
      }
    }

    (closed.result(), WithFreeVariables(Set.empty, openAlong))
  }
}

package scala.compat

import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds

object CompatBuildFrom {

  /** Can be used in place of `implicitly` when the implicit type is a CanBuildFrom to assist in typechecking
    *
    * @example Future.sequence(myFutures)(implicitlyBF, ExecutionContexts.global)
    */
  def implicitlyBF[A, B, M[X] <: TraversableOnce[X]](implicit
    cbf: CanBuildFrom[M[A], B, M[B]]
  ): CanBuildFrom[M[A], B, M[B]] = cbf
}

package com.thatdot.quine

import scala.reflect._
import scala.util.parsing.input.Position
import scala.util.{Failure, Success, Try}

/* TODO

 - consider parsing infix forms of `and()` and `or()`

 - consider adding `.fold()` and `.unfold()`

 - `group(_).by(_)`  (perhaps ok to change syntax)
 * I don't understand how this works yet
 */

/** See [[com.thatdot.quine.gremlin.GremlinQueryRunner]] for the main Gremlin query adapter */
package object gremlin {

  /** Since Gremlin doesn't have types, the values used are all [[Any]]. However, when the time
    * comes to use these values, they need to be casted. Rather than do this ad-hoc with
    * [[asInstanceOf]] or partial functions, this ops class offers a more convenient interface
    * which provides more helpful [[TypeMismatchError]] exceptions
    */
  implicit private[gremlin] class CastOps(any: Any) {
    def castTo[U: ClassTag](expectation: => String, pos: Option[Position] = None): Try[U] = {

      /* NB: trying and catching `asInstanceOf` is _not_ the same thing as this.
       *
       * Sometimes `asInstanceOf` will work locally (for instance if all the
       * types are generic enough) and then blow up when the value actually
       * gets inspected. To mitigate this, we do some quick check here that the
       * types really do line up properly using `asSubclass`
       *
       * This check is further complicated by the fact that `asInstanceOf`
       * happily unboxes/boxes primitive types, but `asSubclass` doesn't take
       * this into account. As a workaround we manually convert the class types
       * to the boxed representation and _then_ use `asSubclass`.
       */

      val boxClassMapping: Map[Class[_], Class[_]] = Map(
        java.lang.Boolean.TYPE -> classTag[java.lang.Boolean].runtimeClass,
        java.lang.Character.TYPE -> classTag[java.lang.Character].runtimeClass,
        java.lang.Byte.TYPE -> classTag[java.lang.Byte].runtimeClass,
        java.lang.Short.TYPE -> classTag[java.lang.Short].runtimeClass,
        java.lang.Integer.TYPE -> classTag[java.lang.Integer].runtimeClass,
        java.lang.Long.TYPE -> classTag[java.lang.Long].runtimeClass,
        java.lang.Float.TYPE -> classTag[java.lang.Float].runtimeClass,
        java.lang.Double.TYPE -> classTag[java.lang.Double].runtimeClass,
      )
      def boxClass(cls: Class[_]): Class[_] = boxClassMapping.getOrElse(cls, cls)

      val exp = classTag[U].runtimeClass
      val act = any.getClass
      val typesTheoreticallyMatch = Try(boxClass(act) asSubclass boxClass(exp)).isSuccess

      Try(any.asInstanceOf[U]) match {
        case Success(u) if typesTheoreticallyMatch => Success(u)
        case _ =>
          Failure(
            TypeMismatchError(
              expected = exp,
              actual = act,
              offender = any,
              explanation = expectation,
              position = pos,
            ),
          )
      }
    }
  }
}

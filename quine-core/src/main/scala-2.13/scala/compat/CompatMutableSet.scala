package scala.compat

import scala.collection.mutable

/** Sets in Scala 2.12 and below expect `addOne` and `subtractOne` to be
  * implemented (but forbid users from overriding `+=` and `-=`)
  */
trait CompatMutableSet[A] extends mutable.Set[A] {
  @inline def addSingle(a: A): this.type
  @inline def subtractSingle(a: A): this.type

  override def addOne(a: A): this.type = addSingle(a)
  override def subtractOne(a: A): this.type = subtractSingle(a)
}

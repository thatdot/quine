package com.thatdot.quine.language.phases

import scala.collection.immutable.{:: => _}

import cats.Monoid
import shapeless.labelled.{FieldType, field}
import shapeless.ops.hlist
import shapeless.{:: => ShCons, HList, HNil, LabelledGeneric, Lazy}

trait Upgrade[A, B] {
  def apply(a: A): B
}

object UpgradeModule {
  implicit class UpgradeOps[A](a: A) {
    def upgradeTo[B](implicit upgrade: Upgrade[A, B]): B =
      upgrade.apply(a)
  }

  def createMonoid[A](zero: A)(add: (A, A) => A): Monoid[A] =
    new Monoid[A] {
      def empty = zero

      def combine(x: A, y: A): A = add(x, y)
    }

  implicit val hnilMonoid: Monoid[HNil] =
    createMonoid[HNil](HNil)((x, y) => HNil)

  implicit def emptyHList[K <: Symbol, H, T <: HList](implicit
    hMonoid: Lazy[Monoid[H]],
    tMonoid: Monoid[T],
  ): Monoid[FieldType[K, H] ShCons T] =
    createMonoid(field[K](hMonoid.value.empty) :: tMonoid.empty) { (x, y) =>
      field[K](hMonoid.value.combine(x.head, y.head)) ::
      tMonoid.combine(x.tail, y.tail)
    }

  implicit def genericUpgrade[
    A,
    B,
    ARepr <: HList,
    BRepr <: HList,
    Common <: HList,
    Added <: HList,
    Unaligned <: HList,
  ](implicit
    aGen: LabelledGeneric.Aux[A, ARepr],
    bGen: LabelledGeneric.Aux[B, BRepr],
    inter: hlist.Intersection.Aux[ARepr, BRepr, Common],
    @annotation.nowarn("cat=unused") diff: hlist.Diff.Aux[BRepr, Common, Added],
    monoid: Monoid[Added],
    prepend: hlist.Prepend.Aux[Added, Common, Unaligned],
    align: hlist.Align[Unaligned, BRepr],
  ): Upgrade[A, B] =
    new Upgrade[A, B] {
      def apply(a: A): B =
        bGen.from(align(prepend(monoid.empty, inter(aGen.to(a)))))
    }
}

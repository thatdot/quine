package com.thatdot.api.v2

trait ShowShort[-A] {
  def showShort(a: A): String
}

trait ShowShortOps {
  implicit class ShortShower[A: ShowShort](a: A) {
    def showShort: String = ShowShort[A].showShort(a)
  }
}

object ShowShort {
  def apply[A](implicit instance: ShowShort[A]): ShowShort[A] = instance

  implicit def eitherShowShort[A: ShowShort, B: ShowShort]: ShowShort[Either[A, B]] =
    (eitherAB: Either[A, B]) => eitherAB.fold(ShowShort[A].showShort, ShowShort[B].showShort)

  implicit def hasErrorsShowShort[A <: HasErrors]: ShowShort[A] =
    (hasErrors: A) => s"[${hasErrors.errors.map(_.message).mkString(", ")}]"

  object syntax extends ShowShortOps
}

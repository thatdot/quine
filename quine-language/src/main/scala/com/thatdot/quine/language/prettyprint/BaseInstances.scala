package com.thatdot.quine.language.prettyprint

import cats.data.NonEmptyList

trait BaseInstances {
  import Doc._

  implicit val stringPrettyPrint: PrettyPrint[String] =
    PrettyPrint.instance(s => text(s))

  implicit val intPrettyPrint: PrettyPrint[Int] =
    PrettyPrint.instance(n => text(n.toString))

  implicit val longPrettyPrint: PrettyPrint[Long] =
    PrettyPrint.instance(n => text(n.toString))

  implicit val doublePrettyPrint: PrettyPrint[Double] =
    PrettyPrint.instance(d => text(d.toString))

  implicit val booleanPrettyPrint: PrettyPrint[Boolean] =
    PrettyPrint.instance(b => text(b.toString))

  implicit val symbolPrettyPrint: PrettyPrint[Symbol] =
    PrettyPrint.instance(s => text("'" + s.name))

  implicit def optionPrettyPrint[A](implicit pp: PrettyPrint[A]): PrettyPrint[Option[A]] =
    PrettyPrint.instance {
      case Some(a) => concat(text("Some("), pp.doc(a), text(")"))
      case None => text("None")
    }

  implicit def listPrettyPrint[A](implicit pp: PrettyPrint[A]): PrettyPrint[List[A]] =
    PrettyPrint.instance { list =>
      if (list.isEmpty) text("[]")
      else {
        val items = list.map(pp.doc)
        concat(
          text("["),
          nest(1, concat(line, intercalate(concat(text(","), line), items))),
          line,
          text("]"),
        )
      }
    }

  implicit def setPrettyPrint[A](implicit pp: PrettyPrint[A]): PrettyPrint[Set[A]] =
    PrettyPrint.instance { set =>
      if (set.isEmpty) text("Set()")
      else {
        val items = set.toList.map(pp.doc)
        concat(
          text("Set("),
          nest(1, concat(line, intercalate(concat(text(","), line), items))),
          line,
          text(")"),
        )
      }
    }

  implicit def mapPrettyPrint[K, V](implicit ppK: PrettyPrint[K], ppV: PrettyPrint[V]): PrettyPrint[Map[K, V]] =
    PrettyPrint.instance { map =>
      if (map.isEmpty) text("Map()")
      else {
        val items = map.toList.map { case (k, v) =>
          concat(ppK.doc(k), text(" -> "), ppV.doc(v))
        }
        concat(
          text("Map("),
          nest(1, concat(line, intercalate(concat(text(","), line), items))),
          line,
          text(")"),
        )
      }
    }

  implicit def eitherPrettyPrint[A, B](implicit ppA: PrettyPrint[A], ppB: PrettyPrint[B]): PrettyPrint[Either[A, B]] =
    PrettyPrint.instance {
      case Left(a) => concat(text("Left("), ppA.doc(a), text(")"))
      case Right(b) => concat(text("Right("), ppB.doc(b), text(")"))
    }

  implicit def nonEmptyListPrettyPrint[A](implicit pp: PrettyPrint[A]): PrettyPrint[NonEmptyList[A]] =
    PrettyPrint.instance { nel =>
      val items = nel.toList.map(pp.doc)
      concat(
        text("NonEmptyList("),
        nest(1, concat(line, intercalate(concat(text(","), line), items))),
        line,
        text(")"),
      )
    }
}

object BaseInstances extends BaseInstances

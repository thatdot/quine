package com.thatdot.quine.util

import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.cypher.Expr.Number

object MonadHelpers {

  implicit class EitherException[L <: Throwable, R](v: Either[L, R]) {
    def getOrThrow: R = v match {
      case Left(e) => throw e
      case Right(value) => value
    }
  }

  implicit class EitherNumber(v: Either[CypherException, Number]) {
    def +(other: Either[CypherException, Number]): Either[CypherException, Number] = (v, other) match {
      case (Right(v1), Right(v2)) => v1 + v2
      case (Left(e), _) => Left(e)
      case (_, Left(e)) => Left(e)
    }

    def +(other: Number): Either[CypherException, Number] = v.flatMap(_ + other)

    def -(other: Either[CypherException, Number]): Either[CypherException, Number] = (v, other) match {
      case (Right(v1), Right(v2)) => v1 - v2
      case (Left(e), _) => Left(e)
      case (_, Left(e)) => Left(e)
    }

    def -(other: Number): Either[CypherException, Number] = v.flatMap(_ - other)

    def *(other: Either[CypherException, Number]): Either[CypherException, Number] = (v, other) match {
      case (Right(v1), Right(v2)) => v1 * v2
      case (Left(e), _) => Left(e)
      case (_, Left(e)) => Left(e)
    }

    def *(other: Number): Either[CypherException, Number] = v.flatMap(_ * other)

    def /(other: Either[CypherException, Number]): Either[CypherException, Number] = (v, other) match {
      case (Right(v1), Right(v2)) => v1 / v2
      case (Left(e), _) => Left(e)
      case (_, Left(e)) => Left(e)
    }

    def /(other: Number): Either[CypherException, Number] = v.flatMap(_ / other)
  }

}

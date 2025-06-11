package com.thatdot.quine.ingest2.transformation

import io.circe.Json
import org.graalvm.polyglot
import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.app.data.{QuineDataFoldablesFrom, QuineDataFoldersTo}
import com.thatdot.quine.app.model.transformation.polyglot.{
  Polyglot,
  PolyglotValueDataFoldableFrom,
  PolyglotValueDataFolderTo,
}
import com.thatdot.quine.graph.{ArbitraryInstances, cypher}

trait FoldableArbitraryHelpers extends ArbitraryInstances {

  val genIntArray: Gen[Json] =
    Gen
      .listOf(Gen.chooseNum(-10000, 10000).map(Json.fromInt))
      .map(xs => Json.arr(xs: _*))

  val genBytes: Gen[Vector[Byte]] = Gen.nonEmptyListOf(Gen.choose(0, 255)).map(_.map(_.toByte).toVector)

  private lazy val genLeaf: Gen[Json] = Gen.oneOf(
    Gen.const(Json.Null),
    Gen.oneOf(Json.True, Json.False),
    Gen.chooseNum(Long.MinValue, Long.MaxValue).map(n => Json.fromLong(n)),
    Gen.alphaStr.map(Json.fromString),
    arbJsonCypher.arbitrary,
  )

  private def genJson(depth: Int): Gen[Json] =
    if (depth == 0) genLeaf
    else
      Gen.oneOf(
        genLeaf,
        Gen.listOf(genJson(depth - 1)).map(xs => Json.arr(xs: _*)),
        Gen
          .mapOf(
            Gen.zip(Gen.identifier, genJson(depth - 1)),
          )
          .map(fields => Json.obj(fields.toSeq: _*)),
      )

  implicit lazy val cypherFrom: DataFoldableFrom[cypher.Value] = QuineDataFoldablesFrom.cypherValueDataFoldable
  implicit lazy val cypherTo: DataFolderTo[cypher.Value] = QuineDataFoldersTo.cypherValueFolder
  implicit lazy val polyFold: DataFoldableFrom[polyglot.Value] = PolyglotValueDataFoldableFrom
  implicit lazy val polyTo: DataFolderTo[Polyglot.HostValue] = PolyglotValueDataFolderTo
  implicit lazy val jsonFrom: DataFoldableFrom[Json] = DataFoldableFrom.jsonDataFoldable
  implicit lazy val jsonTo: DataFolderTo[Json] = DataFolderTo.jsonFolder

  // Some cypher values are not valid for folding, specifically the graph related ones.
  implicit val arbCypher: Arbitrary[cypher.Value] = {
    def noGraph(v: cypher.Value): Boolean = v match {
      case _: cypher.Expr.Node | _: cypher.Expr.Relationship | _: cypher.Expr.Path => false

      case cypher.Expr.List(xs) => xs.forall(value => noGraph(value))
      case cypher.Expr.Map(m) => m.values.forall(value => noGraph(value))
      case _ => true
    }
    Arbitrary(arbCypherValue.arbitrary.retryUntil(noGraph))
  }

  // Generate json from valid cypher values
  val arbJsonCypher: Arbitrary[Json] = Arbitrary(for {
    v <- arbCypher.arbitrary
  } yield convert[cypher.Value, Json](v))

  // Generate json.  Used to ensure deep nested objects are always generated
  implicit val arbJson: Arbitrary[Json] = Arbitrary(genJson(3))

  // Use valid cypher values to generate polyglot values for testing
  implicit val arbPolyglot: Arbitrary[polyglot.Value] = Arbitrary(
    arbCypher.arbitrary.map(value => polyglot.Value.asValue(convert[cypher.Value, Polyglot.HostValue](value))),
  )

  def convert[A, B](in: A)(implicit dataFoldableFrom: DataFoldableFrom[A], dataFolderTo: DataFolderTo[B]): B =
    dataFoldableFrom.fold(in, dataFolderTo)
}

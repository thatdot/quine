package com.thatdot.quine.v2api

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.app.v2api.definitions.ApiUiStyling._
import com.thatdot.quine.{JsonGenerators, ScalaPrimitiveGenerators}

object ApiUiStylingGenerators {
  import ScalaPrimitiveGenerators.Gens.{nonEmptyAlphaNumStr, nonEmptyAlphaStr, optNonEmptyAlphaNumStr, smallNum}
  import JsonGenerators.Gens.dictionary

  object Gens {
    val querySort: Gen[QuerySort] = Gen.oneOf(QuerySort.Node, QuerySort.Text)

    val quickQuery: Gen[QuickQuery] = for {
      name <- nonEmptyAlphaNumStr
      querySuffix <- nonEmptyAlphaNumStr
      sort <- querySort
      edgeLabel <- optNonEmptyAlphaNumStr
    } yield QuickQuery(name, querySuffix, sort, edgeLabel)

    val sampleQuery: Gen[SampleQuery] = for {
      name <- nonEmptyAlphaNumStr
      query <- nonEmptyAlphaNumStr
    } yield SampleQuery(name, query)

    val uiNodePredicate: Gen[UiNodePredicate] = for {
      propertyKeysSize <- smallNum
      propertyKeys <- Gen.containerOfN[Vector, String](propertyKeysSize, nonEmptyAlphaStr)
      knownValues <- dictionary
      dbLabel <- optNonEmptyAlphaNumStr
    } yield UiNodePredicate(propertyKeys, knownValues, dbLabel)

    val uiNodeLabel: Gen[UiNodeLabel] = Gen.oneOf(
      nonEmptyAlphaNumStr.map(UiNodeLabel.Constant(_)),
      for {
        key <- nonEmptyAlphaStr
        prefix <- optNonEmptyAlphaNumStr
      } yield UiNodeLabel.Property(key, prefix),
    )

    val uiNodeAppearance: Gen[UiNodeAppearance] = for {
      predicate <- uiNodePredicate
      size <- Gen.option(Gen.chooseNum(10.0, 100.0))
      icon <- optNonEmptyAlphaNumStr
      color <- optNonEmptyAlphaNumStr
      label <- Gen.option(uiNodeLabel)
    } yield UiNodeAppearance(predicate, size, icon, color, label)

    val uiNodeQuickQuery: Gen[UiNodeQuickQuery] = for {
      predicate <- uiNodePredicate
      quickQueryVal <- quickQuery
    } yield UiNodeQuickQuery(predicate, quickQueryVal)
  }

  object Arbs {
    implicit val querySort: Arbitrary[QuerySort] = Arbitrary(Gens.querySort)
    implicit val quickQuery: Arbitrary[QuickQuery] = Arbitrary(Gens.quickQuery)
    implicit val sampleQuery: Arbitrary[SampleQuery] = Arbitrary(Gens.sampleQuery)
    implicit val uiNodePredicate: Arbitrary[UiNodePredicate] = Arbitrary(Gens.uiNodePredicate)
    implicit val uiNodeLabel: Arbitrary[UiNodeLabel] = Arbitrary(Gens.uiNodeLabel)
    implicit val uiNodeAppearance: Arbitrary[UiNodeAppearance] = Arbitrary(Gens.uiNodeAppearance)
    implicit val uiNodeQuickQuery: Arbitrary[UiNodeQuickQuery] = Arbitrary(Gens.uiNodeQuickQuery)
  }
}

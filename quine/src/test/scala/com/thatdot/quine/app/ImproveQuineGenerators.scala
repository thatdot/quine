package com.thatdot.quine.app

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.ScalaPrimitiveGenerators
import com.thatdot.quine.app.ImproveQuine.{RecipeInfo, TelemetryData}

object ImproveQuineGenerators {
  import ScalaPrimitiveGenerators.Gens.{nonEmptyAlphaNumStr, optNonEmptyAlphaStr, smallPosLong}

  object Gens {
    val recipeInfo: Gen[RecipeInfo] = for {
      recipeNameHash <- nonEmptyAlphaNumStr
      recipeContentsHash <- nonEmptyAlphaNumStr
    } yield RecipeInfo(recipeNameHash, recipeContentsHash)

    val optRecipeInfo: Gen[Option[RecipeInfo]] = Gen.option(recipeInfo)

    val stringList: Gen[List[String]] = Gen.listOf(nonEmptyAlphaNumStr)
    val optStringList: Gen[Option[List[String]]] = Gen.option(stringList)

    val telemetryData: Gen[TelemetryData] = for {
      event <- nonEmptyAlphaNumStr
      service <- nonEmptyAlphaNumStr
      version <- nonEmptyAlphaNumStr
      hostHash <- nonEmptyAlphaNumStr
      time <- nonEmptyAlphaNumStr
      sessionId <- nonEmptyAlphaNumStr
      uptime <- smallPosLong
      persistor <- nonEmptyAlphaNumStr
      sources <- optStringList
      sinks <- optStringList
      recipe <- Arbitrary.arbitrary[Boolean]
      recipeCanonicalName <- optNonEmptyAlphaStr
      recipeInfoOpt <- optRecipeInfo
      apiKey <- optNonEmptyAlphaStr
    } yield TelemetryData(
      event,
      service,
      version,
      hostHash,
      time,
      sessionId,
      uptime,
      persistor,
      sources,
      sinks,
      recipe,
      recipeCanonicalName,
      recipeInfoOpt,
      apiKey,
    )
  }

  object Arbs {
    implicit val recipeInfo: Arbitrary[RecipeInfo] = Arbitrary(Gens.recipeInfo)
    implicit val telemetryData: Arbitrary[TelemetryData] = Arbitrary(Gens.telemetryData)
  }
}

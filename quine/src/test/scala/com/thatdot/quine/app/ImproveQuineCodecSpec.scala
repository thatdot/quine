package com.thatdot.quine.app

import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.ImproveQuine.{RecipeInfo, TelemetryData}

class ImproveQuineCodecSpec extends AnyFunSuite with Matchers with ScalaCheckDrivenPropertyChecks {
  import ImproveQuineGenerators.Arbs._

  test("RecipeInfo encodes with correct field names") {
    forAll { (info: RecipeInfo) =>
      val json = info.asJson
      val obj = json.asObject.get
      obj.keys.toSet shouldBe Set("recipe_name_hash", "recipe_contents_hash")
    }
  }

  test("RecipeInfo encodes field values correctly") {
    forAll { (info: RecipeInfo) =>
      val json = info.asJson
      val obj = json.asObject.get
      obj("recipe_name_hash").flatMap(_.asString) shouldBe Some(info.recipe_name_hash)
      obj("recipe_contents_hash").flatMap(_.asString) shouldBe Some(info.recipe_contents_hash)
    }
  }

  test("TelemetryData encodes with correct field names") {
    forAll { (data: TelemetryData) =>
      val json = data.asJson
      val obj = json.asObject.get
      val expectedFields = Set(
        "event",
        "service",
        "version",
        "host_hash",
        "time",
        "session_id",
        "uptime",
        "persistor",
        "sources",
        "sinks",
        "recipe",
        "recipe_canonical_name",
        "recipe_info",
        "apiKey",
      )
      obj.keys.toSet shouldBe expectedFields
    }
  }

  test("TelemetryData encodes primitive field values correctly") {
    forAll { (data: TelemetryData) =>
      val json = data.asJson
      val obj = json.asObject.get
      obj("event").flatMap(_.asString) shouldBe Some(data.event)
      obj("service").flatMap(_.asString) shouldBe Some(data.service)
      obj("version").flatMap(_.asString) shouldBe Some(data.version)
      obj("host_hash").flatMap(_.asString) shouldBe Some(data.host_hash)
      obj("time").flatMap(_.asString) shouldBe Some(data.time)
      obj("session_id").flatMap(_.asString) shouldBe Some(data.session_id)
      obj("uptime").flatMap(_.asNumber).flatMap(_.toLong) shouldBe Some(data.uptime)
      obj("persistor").flatMap(_.asString) shouldBe Some(data.persistor)
      obj("recipe").flatMap(_.asBoolean) shouldBe Some(data.recipe)
    }
  }

  test("TelemetryData encodes optional fields correctly") {
    forAll { (data: TelemetryData) =>
      val json = data.asJson
      val obj = json.asObject.get

      data.sources match {
        case Some(sources) =>
          obj("sources").flatMap(_.asArray).map(_.flatMap(_.asString).toList) shouldBe Some(sources)
        case None =>
          obj("sources").flatMap(_.asNull) shouldBe Some(())
      }

      data.sinks match {
        case Some(sinks) =>
          obj("sinks").flatMap(_.asArray).map(_.flatMap(_.asString).toList) shouldBe Some(sinks)
        case None =>
          obj("sinks").flatMap(_.asNull) shouldBe Some(())
      }

      data.recipe_canonical_name match {
        case Some(name) =>
          obj("recipe_canonical_name").flatMap(_.asString) shouldBe Some(name)
        case None =>
          obj("recipe_canonical_name").flatMap(_.asNull) shouldBe Some(())
      }

      data.apiKey match {
        case Some(key) =>
          obj("apiKey").flatMap(_.asString) shouldBe Some(key)
        case None =>
          obj("apiKey").flatMap(_.asNull) shouldBe Some(())
      }
    }
  }

  test("TelemetryData encodes nested RecipeInfo correctly") {
    forAll { (data: TelemetryData) =>
      val json = data.asJson
      val obj = json.asObject.get

      data.recipe_info match {
        case Some(info) =>
          val recipeInfoJson = obj("recipe_info").flatMap(_.asObject).get
          recipeInfoJson("recipe_name_hash").flatMap(_.asString) shouldBe Some(info.recipe_name_hash)
          recipeInfoJson("recipe_contents_hash").flatMap(_.asString) shouldBe Some(info.recipe_contents_hash)
        case None =>
          obj("recipe_info").flatMap(_.asNull) shouldBe Some(())
      }
    }
  }
}

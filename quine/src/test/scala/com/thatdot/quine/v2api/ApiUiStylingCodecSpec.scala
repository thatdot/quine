package com.thatdot.quine.v2api

import io.circe.syntax.EncoderOps
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.ArbitraryJson
import com.thatdot.quine.app.v2api.definitions.ApiUiStyling._

class ApiUiStylingCodecSpec
    extends AnyFunSuite
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with ApiUiStylingCodecSpecGenerators {

  test("QuerySort roundtrip encoding/decoding") {
    forAll { (sort: QuerySort) =>
      val json = sort.asJson
      val decoded = json.as[QuerySort]
      decoded shouldBe Right(sort)
    }
  }

  test("QuerySort encodes with type discriminator") {
    (QuerySort.Node: QuerySort).asJson.hcursor.get[String]("type") shouldBe Right("Node")
    (QuerySort.Text: QuerySort).asJson.hcursor.get[String]("type") shouldBe Right("Text")
  }

  test("QuickQuery roundtrip encoding/decoding") {
    forAll { (query: QuickQuery) =>
      val json = query.asJson
      val decoded = json.as[QuickQuery]
      decoded shouldBe Right(query)
    }
  }

  test("QuickQuery encodes with correct field names") {
    forAll { (query: QuickQuery) =>
      val json = query.asJson
      val obj = json.asObject.get
      obj("name").flatMap(_.asString) shouldBe Some(query.name)
      obj("querySuffix").flatMap(_.asString) shouldBe Some(query.querySuffix)
      obj("sort") shouldBe defined
      obj("edgeLabel") shouldBe defined
    }
  }

  test("SampleQuery roundtrip encoding/decoding") {
    forAll { (query: SampleQuery) =>
      val json = query.asJson
      val decoded = json.as[SampleQuery]
      decoded shouldBe Right(query)
    }
  }

  test("SampleQuery encodes with correct field names") {
    forAll { (query: SampleQuery) =>
      val json = query.asJson
      val obj = json.asObject.get
      obj("name").flatMap(_.asString) shouldBe Some(query.name)
      obj("query").flatMap(_.asString) shouldBe Some(query.query)
    }
  }

  test("UiNodePredicate roundtrip encoding/decoding") {
    forAll { (predicate: UiNodePredicate) =>
      val json = predicate.asJson
      val decoded = json.as[UiNodePredicate]
      decoded shouldBe Right(predicate)
    }
  }

  test("UiNodePredicate encodes with correct field names") {
    forAll { (predicate: UiNodePredicate) =>
      val json = predicate.asJson
      val obj = json.asObject.get
      obj("propertyKeys").flatMap(_.asArray).map(_.flatMap(_.asString)) shouldBe Some(predicate.propertyKeys)
      obj("knownValues") shouldBe defined
      obj("dbLabel") shouldBe defined
    }
  }

  test("UiNodeLabel roundtrip encoding/decoding") {
    forAll { (label: UiNodeLabel) =>
      val json = label.asJson
      val decoded = json.as[UiNodeLabel]
      decoded shouldBe Right(label)
    }
  }

  test("UiNodeLabel.Constant encodes with type discriminator") {
    val constant = UiNodeLabel.Constant("test-value")
    val json = (constant: UiNodeLabel).asJson
    json.hcursor.get[String]("type") shouldBe Right("Constant")
    json.hcursor.get[String]("value") shouldBe Right("test-value")
  }

  test("UiNodeLabel.Property encodes with type discriminator") {
    val property = UiNodeLabel.Property("key", Some("prefix: "))
    val json = (property: UiNodeLabel).asJson
    json.hcursor.get[String]("type") shouldBe Right("Property")
    json.hcursor.get[String]("key") shouldBe Right("key")
  }

  test("UiNodeAppearance roundtrip encoding/decoding") {
    forAll { (appearance: UiNodeAppearance) =>
      val json = appearance.asJson
      val decoded = json.as[UiNodeAppearance]
      decoded shouldBe Right(appearance)
    }
  }

  test("UiNodeAppearance encodes with correct field names") {
    forAll { (appearance: UiNodeAppearance) =>
      val json = appearance.asJson
      val obj = json.asObject.get
      obj("predicate") shouldBe defined
      obj("size") shouldBe defined
      obj("icon") shouldBe defined
      obj("color") shouldBe defined
      obj("label") shouldBe defined
    }
  }

  test("UiNodeQuickQuery roundtrip encoding/decoding") {
    forAll { (quickQuery: UiNodeQuickQuery) =>
      val json = quickQuery.asJson
      val decoded = json.as[UiNodeQuickQuery]
      decoded shouldBe Right(quickQuery)
    }
  }

  test("UiNodeQuickQuery encodes with correct field names") {
    forAll { (quickQuery: UiNodeQuickQuery) =>
      val json = quickQuery.asJson
      val obj = json.asObject.get
      obj("predicate") shouldBe defined
      obj("quickQuery") shouldBe defined
    }
  }
}

trait ApiUiStylingCodecSpecGenerators extends ArbitraryJson {

  implicit val genQuerySort: Gen[QuerySort] = Gen.oneOf(QuerySort.Node, QuerySort.Text)
  implicit val arbQuerySort: Arbitrary[QuerySort] = Arbitrary(genQuerySort)

  implicit val genQuickQuery: Gen[QuickQuery] = for {
    name <- genNonEmptyAlphaNumStr
    querySuffix <- genNonEmptyAlphaNumStr
    sort <- genQuerySort
    edgeLabel <- genOptNonEmptyAlphaNumStr
  } yield QuickQuery(name, querySuffix, sort, edgeLabel)
  implicit val arbQuickQuery: Arbitrary[QuickQuery] = Arbitrary(genQuickQuery)

  implicit val genSampleQuery: Gen[SampleQuery] = for {
    name <- genNonEmptyAlphaNumStr
    query <- genNonEmptyAlphaNumStr
  } yield SampleQuery(name, query)
  implicit val arbSampleQuery: Arbitrary[SampleQuery] = Arbitrary(genSampleQuery)

  implicit val genUiNodePredicate: Gen[UiNodePredicate] = for {
    propertyKeysSize <- genSmallNum
    propertyKeys <- Gen.containerOfN[Vector, String](propertyKeysSize, genNonEmptyAlphaStr)
    knownValues <- genJsonDictionary
    dbLabel <- genOptNonEmptyAlphaNumStr
  } yield UiNodePredicate(propertyKeys, knownValues, dbLabel)
  implicit val arbUiNodePredicate: Arbitrary[UiNodePredicate] = Arbitrary(genUiNodePredicate)

  implicit val genUiNodeLabel: Gen[UiNodeLabel] = Gen.oneOf(
    genNonEmptyAlphaNumStr.map(UiNodeLabel.Constant(_)),
    for {
      key <- genNonEmptyAlphaStr
      prefix <- genOptNonEmptyAlphaNumStr
    } yield UiNodeLabel.Property(key, prefix),
  )
  implicit val arbUiNodeLabel: Arbitrary[UiNodeLabel] = Arbitrary(genUiNodeLabel)

  implicit val genUiNodeAppearance: Gen[UiNodeAppearance] = for {
    predicate <- genUiNodePredicate
    size <- Gen.option(Gen.chooseNum(10.0, 100.0))
    icon <- genOptNonEmptyAlphaNumStr
    color <- genOptNonEmptyAlphaNumStr
    label <- Gen.option(genUiNodeLabel)
  } yield UiNodeAppearance(predicate, size, icon, color, label)
  implicit val arbUiNodeAppearance: Arbitrary[UiNodeAppearance] = Arbitrary(genUiNodeAppearance)

  implicit val genUiNodeQuickQuery: Gen[UiNodeQuickQuery] = for {
    predicate <- genUiNodePredicate
    quickQuery <- genQuickQuery
  } yield UiNodeQuickQuery(predicate, quickQuery)
  implicit val arbUiNodeQuickQuery: Arbitrary[UiNodeQuickQuery] = Arbitrary(genUiNodeQuickQuery)
}

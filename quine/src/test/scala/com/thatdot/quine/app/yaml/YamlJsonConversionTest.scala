package com.thatdot.quine.app.yaml

import java.io.InputStream

import scala.util.Using

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.testutil.UJsonGenerators

class YamlJsonConversionTest
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with UJsonGenerators
    with Matchers {

  def loadFile(filename: String)(parse: InputStream => ujson.Value): ujson.Value =
    Using.resource(getClass.getResourceAsStream(filename))(parse)
  def parseYaml(filename: String): ujson.Value = loadFile(filename)(parseToJson)
  def parseUjson(filename: String): ujson.Value = loadFile(filename)(ujson.read(_))

  "The YAML parser" should "parse YAML to equivalent JSON" in {
    parseYaml("/recipes/full.yaml") shouldBe parseUjson("/recipes/full.json")
    parseYaml("/yaml/wikipedia-example.yaml") shouldBe parseUjson("/yaml/wikipedia-example.json")
  }
  it should "parse JSON to equivalent of the same JSON parsed by uJson" in {
    parseYaml("/recipes/full.json") shouldBe parseUjson("/recipes/full.json")
    parseYaml("/yaml/wikipedia-example.json") shouldBe parseUjson("/yaml/wikipedia-example.json")
  }

  "uJson ASTs" should "be round-trippable through YAML" in {
    forAll { (inputJson: ujson.Value) =>
      val yamlJson = new YamlJson
      val yaml = inputJson.transform(yamlJson)
      val transformedJson = yamlJson.transform(yaml, ujson.Value)
      transformedJson shouldBe inputJson
    }
  }

}

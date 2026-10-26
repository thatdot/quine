package com.thatdot.quine.app.model.outputs2.destination

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class NoveltyEndpointsTest extends AnyFunSuite with Matchers {

  test("http(s) URLs with a host are valid base URLs; anything else is not") {
    NoveltyEndpoints.invalidBaseUrl("http://localhost:8382") shouldBe None
    NoveltyEndpoints.invalidBaseUrl(" https://novelty.example.com/ ") shouldBe None
    NoveltyEndpoints.invalidBaseUrl("localhost:8382") shouldBe defined
    NoveltyEndpoints.invalidBaseUrl("ftp://host") shouldBe defined
    NoveltyEndpoints.invalidBaseUrl("http://") shouldBe defined
    NoveltyEndpoints.invalidBaseUrl("") shouldBe defined
  }

  test("the ingest URLs keep the base path and percent-encode the model and ingest name") {
    NoveltyEndpoints.ingestsUri("http://host:8382/", "my model", "a/b").toString shouldBe
    "http://host:8382/api/v2/model/my%20model/ingests/a%2Fb"
    NoveltyEndpoints.ingestsUri("https://host/novelty", "m").toString shouldBe
    "https://host/novelty/api/v2/model/m/ingests"
  }

  test("the Quine ingest WebSocket URL swaps http for ws and https for wss") {
    NoveltyEndpoints.quineIngestWsUri("http://host:8382", "m", "i").toString shouldBe
    "ws://host:8382/api/v2/model/m/ingests/i/quine"
    NoveltyEndpoints.quineIngestWsUri("https://host", "m", "i").toString shouldBe
    "wss://host/api/v2/model/m/ingests/i/quine"
  }

  test("the models listing URL keeps the base path") {
    NoveltyEndpoints.modelsUri("https://host/novelty/").toString shouldBe "https://host/novelty/api/v2/system/models"
  }

  test("a model name must be one Novelty can hold, ignoring case") {
    NoveltyEndpoints.invalidModel("default") shouldBe None
    NoveltyEndpoints.invalidModel("Model2") shouldBe None
    NoveltyEndpoints.invalidModel("my-model") shouldBe defined
    NoveltyEndpoints.invalidModel("2model") shouldBe defined
    NoveltyEndpoints.invalidModel("") shouldBe defined
    NoveltyEndpoints.invalidModel("abcdefghijklmnopq") shouldBe defined
  }
}

package com.thatdot.quine.webapp.queryui

import io.circe.Json
import io.circe.syntax._
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.webapp.History
import com.thatdot.quine.webapp.resultspanel.cards.{CardSnapshot, CardViewerSnapshot}

/** The containment guarantee on [[FullSnapshot]]'s `cards` field: a card this build can no
  * longer decode costs the user that card and nothing else.
  *
  * `ExplorerStore.load` discards *and deletes* a snapshot whose decode fails, and the next
  * periodic save writes the empty canvas over it — so a `CardSnapshot` field renamed between
  * releases would, without this, silently and unrecoverably take the graph, history, pins,
  * and viewport with it on the user's first reload after upgrading.
  */
class ExplorerStoreCodecTest extends AnyFunSuite with Matchers with EitherValues {

  private val node = FullSerializableNode(
    id = "n1",
    hostIndex = 0,
    label = "Person",
    properties = Map("name" -> Json.fromString("ada")),
    x = Some(12d),
    y = Some(34d),
    fixed = false,
  )

  /** Everything in a snapshot that isn't cards — the state a bad card must not endanger. */
  private val snapshot = FullSnapshot(
    nodes = Seq(node),
    edges = Nil,
    history = History.empty[QueryUiEvent],
    query = "MATCH (n) RETURN n",
    foundNodesCount = Some(1),
    foundEdgesCount = None,
    atTime = None,
    pinnedNodes = Set("n1"),
    viewPosition = (10d, 20d),
    viewScale = 1.5,
    layout = "standard",
    collapsedClusters = Nil,
    clusterPositions = Map.empty,
    resultsEntries = Nil,
    resultsCurrentIdx = 0,
    resultsCollapsed = false,
    cards = Nil,
    expandedCardIdx = None,
    savedAt = 1000d,
  )

  private val card = CardSnapshot(
    kind = CardSnapshot.KindAdhoc,
    sampleSize = 10,
    hasSampleLimit = true,
    stopped = false,
    viewer = CardViewerSnapshot(
      view = "table",
      csvFlat = false,
      search = "",
      sortCol = None,
      sortDir = "asc",
      colWidths = Vector.empty,
    ),
    query = "MATCH (n) RETURN n",
    language = "cypher",
    outcome = None,
    sqName = "",
    tapPoint = "",
    tapQueryText = None,
  )

  /** The regression this guards: `hasSampleLimit` is required, so a snapshot written before
    * it replaced `live` has no such key and the derived `CardSnapshot` decoder rejects it.
    */
  private val staleCardJson: Json = card.asJson.mapObject(_.remove("hasSampleLimit"))

  /** Re-encode `snapshot` with `cards` replaced by a raw JSON value. */
  private def snapshotWithCards(cards: Json): Json =
    snapshot.asJson.mapObject(_.add("cards", cards))

  private def decode(json: Json): FullSnapshot =
    json.as[FullSnapshot].value

  /** Assert that everything other than `cards` came back intact. */
  private def assertGraphSurvived(decoded: FullSnapshot): Unit = {
    decoded.nodes shouldBe Seq(node)
    decoded.query shouldBe "MATCH (n) RETURN n"
    decoded.pinnedNodes shouldBe Set("n1")
    decoded.viewPosition shouldBe ((10d, 20d))
    decoded.viewScale shouldBe 1.5
    decoded.layout shouldBe "standard"
    ()
  }

  test("a snapshot round-trips with its cards") {
    val decoded = decode(snapshotWithCards(Json.arr(card.asJson)))

    decoded.cards shouldBe Seq(card)
    assertGraphSurvived(decoded)
  }

  test("an undecodable card is dropped, and the rest of the snapshot survives") {
    val decoded = decode(snapshotWithCards(Json.arr(staleCardJson)))

    decoded.cards shouldBe empty
    assertGraphSurvived(decoded)
  }

  test("an undecodable card doesn't take its readable neighbours with it") {
    val decoded = decode(snapshotWithCards(Json.arr(card.asJson, staleCardJson, card.asJson)))

    decoded.cards shouldBe Seq(card, card)
    assertGraphSurvived(decoded)
  }

  test("a `cards` value that isn't an array yields no cards rather than failing") {
    val decoded = decode(snapshotWithCards(Json.fromString("not an array")))

    decoded.cards shouldBe empty
    assertGraphSurvived(decoded)
  }

  test("a snapshot written before the card system, with no `cards` key at all, still decodes") {
    val preCards = snapshot.asJson.mapObject(_.remove("cards"))
    val decoded = decode(preCards)

    decoded.cards shouldBe empty
    assertGraphSurvived(decoded)
  }

  test("a dropped card also clears the expanded index, which could address the wrong card") {
    // Saved as [stale, card] with index 1 expanded. The drop shifts `card` to position 0,
    // so a surviving index 1 would expand whatever card sits there now — not the one saved.
    val json = snapshotWithCards(Json.arr(staleCardJson, card.asJson))
      .mapObject(_.add("expandedCardIdx", Json.fromInt(1)))

    val decoded = decode(json)

    decoded.cards shouldBe Seq(card)
    decoded.expandedCardIdx shouldBe None
    assertGraphSurvived(decoded)
  }

  test("the expanded index survives a decode that drops no cards") {
    val json = snapshotWithCards(Json.arr(card.asJson, card.asJson))
      .mapObject(_.add("expandedCardIdx", Json.fromInt(1)))

    val decoded = decode(json)

    decoded.cards shouldBe Seq(card, card)
    decoded.expandedCardIdx shouldBe Some(1)
    assertGraphSurvived(decoded)
  }
}

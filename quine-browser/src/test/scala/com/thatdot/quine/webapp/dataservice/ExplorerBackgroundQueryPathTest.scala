package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.{Observer, Signal}
import com.raquo.airstream.eventbus.EventBus
import com.raquo.airstream.ownership.ManualOwner
import com.raquo.airstream.state.Var
import io.circe.Json
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.webapp.queryui.PanelTapSubscriptions
import com.thatdot.quine.webapp.resultspanel.TapTarget
import com.thatdot.quine.webapp.resultspanel.streaming.LiveStream
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2GraphFeed

/** End-to-end over the Explorer's background-query data path, from a tap handler receiving a row
  * to the buffer a result card renders:
  *
  * {{{ handler.push → backgroundQueryTapsSignal → PanelTapSubscriptions.sources
  *      → LiveSource.records → LiveStream.rows }}}
  *
  * Every link here has been re-plumbed at least once (the handler grew a message buffer, the
  * adapter moved out to be shared with the Streams page, the store gained subscriber
  * refcounting), and a break anywhere along it shows up only as a results card that stays empty
  * — no error, nothing in the console.
  */
class ExplorerBackgroundQueryPathTest extends AnyFunSuite with Matchers with OptionValues {

  private val owner = new ManualOwner

  /** Just enough of the service for [[PanelTapSubscriptions]]; only the background-query half
    * is exercised.
    */
  final private class StubWiretapService(
    taps: Var[Map[String, BackgroundQueryTapHandler]],
  ) extends WiretapService {
    val wiretapDispatch: Observer[WiretapService.Command] = Observer.empty
    val wiretapsSignal: Signal[Map[WiretapOwner, List[WiretapHandler]]] =
      Var(Map.empty[WiretapOwner, List[WiretapHandler]]).signal
    val enabledGraphFeedsSignal: Signal[Map[String, V2GraphFeed]] = Var(Map.empty[String, V2GraphFeed]).signal
    val backgroundQueryTapsSignal: Signal[Map[String, BackgroundQueryTapHandler]] = taps.signal
  }

  private def handler(executionId: String, displayName: String = "sweep") =
    new BackgroundQueryTapHandler(
      executionId = executionId,
      displayName = displayName,
      status = Var(BqTapStatus.Live),
      rowCount = Var(0L),
      messages = Var(Seq.empty),
      rowsBus = new EventBus[Json],
    )

  private def row(i: Int): Json = Json.obj("n.i" -> Json.fromInt(i))

  test("a background-query tap surfaces as a LiveSource the card layer can find") {
    val taps = Var(Map.empty[String, BackgroundQueryTapHandler])
    val subscriptions = new PanelTapSubscriptions(new StubWiretapService(taps), WiretapOwner("explorer"))

    var sources = Vector.empty[com.thatdot.quine.webapp.resultspanel.LiveSource]
    subscriptions.sources.foreach(sources = _)(owner)

    taps.set(Map("exec-1" -> handler("exec-1")))

    sources should have size 1
    // The card layer keys everything off `tapTarget`; a source without one is invisible to it.
    sources.head.tapTarget.value shouldBe TapTarget.BackgroundQuery("exec-1", "sweep")
    sources.head.tapTarget.value.key shouldBe "bq:exec-1"
  }

  test("rows pushed to the handler reach the source's record stream") {
    val h = handler("exec-1")
    val taps = Var(Map("exec-1" -> h))
    val subscriptions = new PanelTapSubscriptions(new StubWiretapService(taps), WiretapOwner("explorer"))

    var sources = Vector.empty[com.thatdot.quine.webapp.resultspanel.LiveSource]
    subscriptions.sources.foreach(sources = _)(owner)

    var received = Vector.empty[Json]
    sources.head.records.foreach(received :+= _)(owner)

    h.push(row(1).noSpaces, row(1))
    h.push(row(2).noSpaces, row(2))

    received shouldBe Vector(row(1), row(2))
  }

  test("those rows land in the LiveStream buffer a results card renders") {
    // The whole chain, exactly as `QueryUi.connectBudgetedTapStream` wires it for a background
    // query: unbudgeted (background-query cards open Live), drained on freeze.
    val h = handler("exec-1")
    val taps = Var(Map("exec-1" -> h))
    val subscriptions = new PanelTapSubscriptions(new StubWiretapService(taps), WiretapOwner("explorer"))

    var sources = Vector.empty[com.thatdot.quine.webapp.resultspanel.LiveSource]
    subscriptions.sources.foreach(sources = _)(owner)

    val stream = new LiveStream
    stream.connect(sources.head.records, budget = () => None)

    (1 to 3).foreach(i => h.push(row(i).noSpaces, row(i)))
    stream.freeze() // drains the throttle buffer

    stream.rows.now().map(_.data) shouldBe Vector(row(1), row(2), row(3))
    stream.columns.now() shouldBe Vector("n.i")
  }

  test("the message log the Streams inspection reads does not interfere with the row stream") {
    // Both consumers hang off the same `push`; the log was added later, and appending to it
    // must not swallow or reorder what the Explorer's card receives.
    val h = handler("exec-1")
    val taps = Var(Map("exec-1" -> h))
    val subscriptions = new PanelTapSubscriptions(new StubWiretapService(taps), WiretapOwner("explorer"))

    var sources = Vector.empty[com.thatdot.quine.webapp.resultspanel.LiveSource]
    subscriptions.sources.foreach(sources = _)(owner)

    var received = Vector.empty[Json]
    sources.head.records.foreach(received :+= _)(owner)

    (1 to 3).foreach(i => h.push(row(i).noSpaces, row(i)))

    received should have size 3
    h.messages.now() should have size 3
    h.rowCount.now() shouldBe 3L
  }

  test("standing-query and background-query sources coexist in one list") {
    // `sources` combines two signals; a regression there would drop one kind silently.
    val taps = Var(Map("exec-1" -> handler("exec-1"), "exec-2" -> handler("exec-2", "other")))
    val subscriptions = new PanelTapSubscriptions(new StubWiretapService(taps), WiretapOwner("explorer"))

    var sources = Vector.empty[com.thatdot.quine.webapp.resultspanel.LiveSource]
    subscriptions.sources.foreach(sources = _)(owner)

    sources.flatMap(_.tapTarget).map(_.key).toSet shouldBe Set("bq:exec-1", "bq:exec-2")
  }
}

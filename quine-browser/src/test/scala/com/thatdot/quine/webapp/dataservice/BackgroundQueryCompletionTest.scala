package com.thatdot.quine.webapp.dataservice

import io.circe.parser.parse
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The one branch every background-query result card depends on: telling the tap's terminal
  * frame apart from a result row.
  *
  * Getting it wrong in either direction is invisible at compile time and obvious to a user — a
  * missed sentinel puts a `__backgroundQueryComplete` column in the results table and leaves the
  * card claiming to still be running; a false positive silently drops a row.
  *
  * The frames here are captured verbatim from a running server's
  * `.../backgroundQueries/{id}:tap` socket.
  */
class BackgroundQueryCompletionTest extends AnyFunSuite with Matchers with OptionValues {

  private def frame(json: String) = parse(json).toOption.value

  test("the terminal frame is recognized and its fields decoded") {
    val completion = BackgroundQueryCompletion
      .unapply(
        frame(
          """{"__backgroundQueryComplete":
            |{"status":"completed","totalRowCount":3000,"droppedBufferedRows":0,"error":null}}""".stripMargin,
        ),
      )
      .value

    completion.status shouldBe "completed"
    completion.totalRowCount.value shouldBe 3000L
    completion.droppedBufferedRows shouldBe 0L
    completion.error shouldBe None
  }

  test("a failed run carries its error through") {
    val completion = BackgroundQueryCompletion
      .unapply(
        frame(
          """{"__backgroundQueryComplete":
            |{"status":"failed","totalRowCount":null,"droppedBufferedRows":7,"error":"boom"}}""".stripMargin,
        ),
      )
      .value

    completion.status shouldBe "failed"
    completion.totalRowCount shouldBe None
    completion.droppedBufferedRows shouldBe 7L
    completion.error.value shouldBe "boom"
  }

  test("a result row is never mistaken for the terminal frame") {
    BackgroundQueryCompletion.unapply(frame("""{"n.i":511,"doubled":1022}""")) shouldBe None
    // A row whose column is *named* like the sentinel's fields still isn't one.
    BackgroundQueryCompletion.unapply(frame("""{"status":"completed","totalRowCount":3}""")) shouldBe None
    // Non-object frames (a transformation can emit a scalar or array) are rows too.
    BackgroundQueryCompletion.unapply(frame("""[1,2,3]""")) shouldBe None
    BackgroundQueryCompletion.unapply(frame("""7""")) shouldBe None
  }

  test("a malformed terminal frame still terminates rather than becoming a row") {
    // The key is unambiguous, so the frame is the terminator even if its body drifts — rendering
    // `__backgroundQueryComplete` as a table column would be strictly worse than defaulting.
    val completion = BackgroundQueryCompletion.unapply(frame("""{"__backgroundQueryComplete":{}}""")).value
    completion.status shouldBe "completed"
    completion.droppedBufferedRows shouldBe 0L
  }
}

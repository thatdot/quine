package com.thatdot.quine.graph

import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class EventTimeTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks with ArbitraryInstances {

  test("Largest in this ms") {
    forAll { et: EventTime =>
      assert(
        et.largestEventTimeInThisMillisecond == EventTime(
          et.millis,
          16383, //10 ** 14 -1
          255 //10 ** 8 -1
        )
      )
    }
  }

  test("tick") {

    forAll { et: EventTime =>

      val t1 = et.tick(mustAdvanceLogicalTime = true, et.millis + 1)
      assert(t1 == EventTime(et.millis + 1L, 0L, 0L))
      val t2 = et.tick(mustAdvanceLogicalTime = true, et.millis - 1)
      assert(t2 == EventTime(et.millis - 1L, 0L, 0L))

      val t3 = et.tick(mustAdvanceLogicalTime = true, et.millis)
      assert(t3 == EventTime(et.millis, et.timestampSequence + 1L, 0L))

      val t4 = et.tick(mustAdvanceLogicalTime = false, et.millis)
      assert(t4 == EventTime(et.millis, et.timestampSequence, 0L))

      val t5 = et.tick(mustAdvanceLogicalTime = false, et.millis + 1)
      assert(t5 == EventTime(et.millis + 1L, 0L, 0L))

      val t6 = et.tick(mustAdvanceLogicalTime = false, et.millis - 1)
      assert(t6 == EventTime(et.millis - 1L, 0L, 0L))
    }

  }

}

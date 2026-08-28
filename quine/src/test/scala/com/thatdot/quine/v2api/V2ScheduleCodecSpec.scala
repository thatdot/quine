package com.thatdot.quine.v2api

import java.time.{DayOfWeek, Instant, LocalTime}

import scala.concurrent.duration.DurationInt

import io.circe.syntax._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.v2api.endpoints.Schedule

class V2ScheduleCodecSpec extends AnyFunSpec with Matchers {

  private val samples: Seq[Schedule] = Seq(
    Schedule.Hourly(minute = 15),
    Schedule.Daily(LocalTime.of(9, 30), timezone = "America/New_York"),
    Schedule.Daily(LocalTime.of(9, 30, 15)), // to the second
    Schedule.Weekly(DayOfWeek.WEDNESDAY, LocalTime.of(6, 0)),
    Schedule.Monthly(dayOfMonth = 1, LocalTime.MIDNIGHT, timezone = "Europe/London"),
    Schedule.Interval(90.seconds, startAt = None),
    Schedule.Interval(1.hour + 30.minutes, startAt = Some(Instant.parse("2021-01-01T00:00:00Z"))),
  )

  describe("Schedule codec") {
    it("round-trips every variant") {
      samples.foreach { schedule =>
        schedule.asJson.as[Schedule] shouldBe Right(schedule)
      }
    }

    it("uses a \"type\" discriminator naming the variant") {
      (Schedule.Hourly(0): Schedule).asJson.hcursor.get[String]("type") shouldBe Right("Hourly")
      (Schedule.Interval(90.seconds, None): Schedule).asJson.hcursor.get[String]("type") shouldBe Right("Interval")
    }

    it("encodes the interval as an AIP-142 duration string and the anchor as an RFC-3339 instant") {
      val json = (Schedule.Interval(90.seconds, Some(Instant.parse("2021-01-01T00:00:00Z"))): Schedule).asJson
      json.hcursor.get[String]("every") shouldBe Right("1m30s")
      json.hcursor.get[String]("startAt") shouldBe Right("2021-01-01T00:00:00Z")
    }

    it("encodes the day of week as a screaming-snake string") {
      (Schedule.Weekly(DayOfWeek.MONDAY, LocalTime.of(9, 0)): Schedule).asJson.hcursor
        .get[String]("dayOfWeek") shouldBe Right("MONDAY")
    }

    it("encodes the time of day as an ISO \"HH:mm\" string") {
      (Schedule.Daily(LocalTime.of(9, 30)): Schedule).asJson.hcursor.get[String]("at") shouldBe Right("09:30")
      (Schedule.Daily(LocalTime.of(9, 30, 15)): Schedule).asJson.hcursor.get[String]("at") shouldBe Right("09:30:15")
    }
  }
}

package com.thatdot.quine.compiler.cypher

import java.time.{Duration => JavaDuration, LocalDateTime => JavaLocalDateTime, ZonedDateTime => JavaZonedDateTime}

import com.thatdot.quine.graph.cypher.Expr

class CypherDates extends CypherHarness("cypher-dates-tests") {

  describe("construct dates/durations from options") {
    testExpression(
      "localdatetime({ year: 2019 })",
      Expr.LocalDateTime(JavaLocalDateTime.of(2019, 1, 1, 0, 0)),
      expectedIsIdempotent = false
    )

    testExpression(
      "localdatetime({ year: 1995, month: 4, day: 24 })",
      Expr.LocalDateTime(JavaLocalDateTime.of(1995, 4, 24, 0, 0)),
      expectedIsIdempotent = false
    )

    testExpression(
      "datetime({ epochSeconds: 1607532063, timezone: 'UTC' }).ordinalDay",
      Expr.Integer(344L),
      expectedIsIdempotent = false
    )

    testExpression(
      "duration({ days: 24 })",
      Expr.Duration(JavaDuration.ofDays(24)),
      expectedIsIdempotent = true
    )
  }

  describe("construct dates/durations from strings") {
    testExpression(
      "datetime('2020-12-09T13:15:41.914-05:00[America/Montreal]')",
      Expr.DateTime(JavaZonedDateTime.parse("2020-12-09T13:15:41.914-05:00[America/Montreal]")),
      expectedIsIdempotent = false
    )

    testExpression(
      "localdatetime('2020-12-09T13:15:41.914')",
      Expr.LocalDateTime(JavaLocalDateTime.parse("2020-12-09T13:15:41.914")),
      expectedIsIdempotent = false
    )

    testExpression(
      "duration('PT20.345S')",
      Expr.Duration(JavaDuration.parse("PT20.345S")),
      expectedIsIdempotent = true
    )
  }

  describe("extract parts of dates/durations as properties") {
    val components = List(
      "year" -> 1995L,
      "quarter" -> 2L,
      "month" -> 4L,
      "week" -> 17L,
      "dayOfQuarter" -> 24L,
      "day" -> 24L,
      "ordinalDay" -> 114L,
      "dayOfWeek" -> 1L,
      "hour" -> 0L,
      "minute" -> 0L,
      "second" -> 0L,
      "millisecond" -> 0L,
      "microsecond" -> 0L,
      "nanosecond" -> 0L
    )
    for ((name, value) <- components)
      testExpression(
        s"localdatetime({ year: 1995, month: 4, day: 24 }).$name",
        Expr.Integer(value),
        expectedIsIdempotent = false
      )

    testExpression(
      "datetime({ year: 1995, month: 4, day: 24, timezone: 'Asia/Hong_Kong' }).epochSeconds",
      Expr.Integer(798652800L),
      expectedIsIdempotent = false
    )
  }

  describe("durations computed from dates") {
    testExpression(
      """duration.between(
        |  localdatetime({ year: 1995, month: 4, day: 24, hour: 3, minute: 2 }),
        |  localdatetime({ year: 1995, month: 4, day: 25, hour: 5, minute: 1, second: 53 })
        |)""".stripMargin,
      Expr.Duration(JavaDuration.parse("PT25H59M53S")),
      expectedIsIdempotent = false
    )

    testExpression(
      """duration.between(
        |  datetime({ epochSeconds: 1372231111, timezone: 'UTC' }),
        |  datetime({ epochSeconds: 1372231111, timezone: 'America/Montreal' })
        |)""".stripMargin,
      Expr.Duration(JavaDuration.ofMillis(0)),
      expectedIsIdempotent = false
    )

    testExpression(
      """duration.between(
        |  datetime({ epochSeconds: 798652800, timezone: 'Asia/Hong_Kong' }),
        |  datetime({ year: 1995, month: 4, day: 24, timezone: 'America/Montreal' })
        |)""".stripMargin,
      Expr.Duration(JavaDuration.ofHours(12)),
      expectedIsIdempotent = false
    )
  }

  describe("comparision") {
    testExpression(
      "datetime({ epochSeconds: 798652800 }) = datetime({ epochSeconds: 798652800 })",
      Expr.True,
      expectedIsIdempotent = false
    )

    testExpression(
      "localdatetime({ year: 2001, month: 11 }) < localdatetime({ year: 2000, month: 10, day: 2 })",
      Expr.False,
      expectedIsIdempotent = false
    )
  }

  describe("duration arithmetic") {
    testExpression(
      "(datetime({ year: 2001 }) + duration({ days: 13, hours: 1 })).day",
      Expr.Integer(14L),
      expectedIsIdempotent = false
    )

    testExpression(
      "(duration({ days: 13, hours: 1 }) + datetime({ year: 2001 })).hour",
      Expr.Integer(1L),
      expectedIsIdempotent = false
    )

    testExpression(
      "(datetime({ year: 2001 }) - duration({ days: 13, hours: 1 })).dayOfQuarter",
      Expr.Integer(79L),
      expectedIsIdempotent = false
    )

    testExpression(
      "duration({ minutes: 361 }) + duration({ days: 14 })",
      Expr.Duration(JavaDuration.parse("PT342H1M")),
      expectedIsIdempotent = true
    )

    testExpression(
      "duration({ minutes: 361 }) - duration({ days: 14 })",
      Expr.Duration(JavaDuration.parse("PT-329H-59M")),
      expectedIsIdempotent = true
    )
  }

  describe("parsing and pretty printing with custom formats") {
    testExpression(
      "temporal.format(datetime('Mon, 1 Apr 2019 11:05:30 GMT', 'E, d MMM yyyy HH:mm:ss z'), 'MMM dd uu')",
      Expr.Str("Apr 01 19"),
      expectedIsIdempotent = false
    )

    testExpression(
      "temporal.format(localdatetime('Apr 1, 11 oclock in \\'19', 'MMM d, HH \\'oclock in \\'\\'\\'yy'), 'MMM dd uu')",
      Expr.Str("Apr 01 19"),
      expectedIsIdempotent = false
    )
  }
}

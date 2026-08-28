package com.thatdot.quine.graph.scheduledjob

import java.time.{DayOfWeek, LocalTime}

import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.model.Milliseconds

class JobScheduleTest extends AnyFunSuite {

  private def ms(t: Long) = Milliseconds(t)
  // Every hour on the hour (minute 0), evaluated in UTC.
  private val hourly = ScheduleSpec.Hourly(minute = 0, zoneId = "UTC")

  // 2021-01-01T00:00:00Z, and a few offsets from it.
  private val epochHour = 1609459200000L
  private val oneHour = 3600000L

  private def state(next: Long, inProgress: Option[Long] = None) =
    ScheduledJobState(
      jobType = "test",
      payload = "{}",
      schedule = hourly,
      nextFireAt = Some(ms(next)),
      inProgressSince = inProgress.map(ms),
    )

  test("dueJobs returns jobs whose nextFireAt has passed and are not running") {
    val states = Map(
      "past" -> state(next = 50),
      "exact" -> state(next = 100),
      "future" -> state(next = 200),
      "past-but-running" -> state(next = 50, inProgress = Some(10)),
    )
    assert(JobSchedule.dueJobs(states, ms(100)) == Set("past", "exact"))
  }

  test("nextDeadline is the earliest fire time among non-running jobs") {
    val states = Map(
      "a" -> state(next = 300),
      "b" -> state(next = 150),
      "running" -> state(next = 10, inProgress = Some(5)), // excluded despite being earliest
    )
    assert(JobSchedule.nextDeadline(states).contains(ms(150)))
    assert(JobSchedule.nextDeadline(Map.empty).isEmpty)
    assert(JobSchedule.nextDeadline(Map("r" -> state(next = 10, inProgress = Some(5)))).isEmpty)
  }

  test("onFire marks running, records the fire, and advances to the next slot") {
    val now = ms(epochHour + 30 * 60 * 1000) // 00:30
    val fired = JobSchedule.onFire(state(next = epochHour), now)
    assert(fired.inProgressSince.contains(now))
    assert(fired.lastFireAt.contains(now))
    assert(fired.nextFireAt.contains(ms(epochHour + oneHour))) // next top of the hour: 01:00, skipping 00:00
  }

  test("a late fire advances to the next slot instead of drifting") {
    // Fired at 00:05 for the 00:00 slot: the next fire is the 01:00 slot, phase-stable — not now + 1h.
    val fired = JobSchedule.onFire(state(next = epochHour), ms(epochHour + 5 * 60 * 1000))
    assert(fired.nextFireAt.contains(ms(epochHour + oneHour)))
  }

  test("Hourly.nextFireAfter returns the next matching slot strictly after `from`") {
    assert(hourly.nextFireAfter(ms(epochHour)).contains(ms(epochHour + oneHour)), "strictly after the 00:00 slot")
    assert(hourly.nextFireAfter(ms(epochHour + 30 * 60 * 1000)).contains(ms(epochHour + oneHour)), "00:30 -> 01:00")
  }

  test("firstFireAt is immediately due when activation lands exactly on a slot, else the next slot") {
    assert(hourly.firstFireAt(ms(epochHour)).contains(ms(epochHour)), "activation exactly on the hour fires now")
    assert(
      hourly.firstFireAt(ms(epochHour + 1)).contains(ms(epochHour + oneHour)),
      "1ms past the slot waits for the next",
    )
  }

  test("Daily is evaluated in the job's timezone") {
    // Noon daily. On 2021-01-01, New York is EST (UTC-5), so its noon is 17:00Z.
    val noonUtc = ScheduleSpec.Daily(LocalTime.NOON, zoneId = "UTC")
    val noonNy = ScheduleSpec.Daily(LocalTime.NOON, zoneId = "America/New_York")
    val startOfDay = ms(epochHour) // 2021-01-01T00:00:00Z
    assert(noonUtc.firstFireAt(startOfDay).contains(ms(epochHour + 12 * oneHour)), "12:00Z")
    assert(noonNy.firstFireAt(startOfDay).contains(ms(epochHour + 17 * oneHour)), "12:00 EST = 17:00Z")
  }

  test("Daily fires to the second") {
    // 09:30:15 daily.
    val precise = ScheduleSpec.Daily(LocalTime.of(9, 30, 15), zoneId = "UTC")
    assert(precise.firstFireAt(ms(epochHour)).contains(ms(epochHour + 9 * oneHour + 30 * 60 * 1000 + 15 * 1000)))
  }

  test("Weekly fires on the given day of week") {
    // 2021-01-01 is a Friday. The next Monday 09:00Z is 2021-01-04T09:00:00Z = epochHour + 3 days + 9h.
    val mondayMorning = ScheduleSpec.Weekly(DayOfWeek.MONDAY, LocalTime.of(9, 0), zoneId = "UTC")
    assert(mondayMorning.firstFireAt(ms(epochHour)).contains(ms(epochHour + 3 * 24 * oneHour + 9 * oneHour)))
  }

  test("Monthly fires on the given day of month") {
    // Next 15th at 00:00Z after Jan 1 is 2021-01-15T00:00:00Z = epochHour + 14 days.
    val fifteenth = ScheduleSpec.Monthly(dayOfMonth = 15, LocalTime.MIDNIGHT, zoneId = "UTC")
    assert(fifteenth.firstFireAt(ms(epochHour)).contains(ms(epochHour + 14 * 24 * oneHour)))
  }

  test("an out-of-range (unvalidated) field yields a schedule that never fires") {
    val bad = ScheduleSpec.Hourly(minute = 99, zoneId = "UTC")
    assert(bad.nextFireAfter(ms(epochHour)).isEmpty)
    assert(bad.firstFireAt(ms(epochHour)).isEmpty)
  }

  // -- Interval --------------------------------------------------------------

  private val everyHour = FiniteDuration(oneHour, MILLISECONDS)

  test("Interval fires at anchor + k·every") {
    val interval = ScheduleSpec.Interval(everyHour, startAt = Some(ms(epochHour)))
    assert(interval.firstFireAt(ms(epochHour)).contains(ms(epochHour)), "anchor exactly at activation fires now (k=0)")
    assert(interval.nextFireAfter(ms(epochHour)).contains(ms(epochHour + oneHour)), "strictly after the anchor")
    assert(interval.nextFireAfter(ms(epochHour + 30 * 60 * 1000)).contains(ms(epochHour + oneHour)), "00:30 -> 01:00")
  }

  test("Interval with a future anchor waits until the anchor") {
    val interval = ScheduleSpec.Interval(everyHour, startAt = Some(ms(epochHour + 2 * oneHour)))
    assert(interval.firstFireAt(ms(epochHour)).contains(ms(epochHour + 2 * oneHour)))
  }

  test("Interval with a past anchor collapses missed slots to the next multiple") {
    val interval = ScheduleSpec.Interval(everyHour, startAt = Some(ms(epochHour)))
    // 90 minutes after the anchor: the next multiple at or after is the 2h slot.
    assert(interval.firstFireAt(ms(epochHour + 90 * 60 * 1000)).contains(ms(epochHour + 2 * oneHour)))
  }

  test("anchoredAt fills a missing Interval anchor with `now`; other variants are unchanged") {
    val unanchored = ScheduleSpec.Interval(everyHour, startAt = None)
    assert(unanchored.anchoredAt(ms(epochHour)) == ScheduleSpec.Interval(everyHour, Some(ms(epochHour))))
    // A default-anchored interval fires immediately on creation, then every interval.
    val anchored = unanchored.anchoredAt(ms(epochHour))
    assert(anchored.firstFireAt(ms(epochHour)).contains(ms(epochHour)))
    assert(anchored.nextFireAfter(ms(epochHour)).contains(ms(epochHour + oneHour)))
    assert(hourly.anchoredAt(ms(epochHour)) == hourly, "wall-clock variants are unaffected by anchoredAt")
  }

  // -- Validation ------------------------------------------------------------

  test("validate accepts in-range schedules and rejects out-of-range ones") {
    assert(ScheduleSpec.validate(hourly) == Right(hourly))
    assert(ScheduleSpec.validate(ScheduleSpec.Hourly(60, "UTC")).isLeft, "minute out of range")
    assert(ScheduleSpec.validate(ScheduleSpec.Monthly(0, LocalTime.MIDNIGHT, "UTC")).isLeft, "dayOfMonth out of range")
    assert(ScheduleSpec.validate(ScheduleSpec.Hourly(0, "Not/AZone")).isLeft, "unknown timezone rejected")
  }

  test("validate rejects an interval shorter than the minimum") {
    assert(ScheduleSpec.validate(ScheduleSpec.Interval(FiniteDuration(500, MILLISECONDS), None)).isLeft)
    assert(ScheduleSpec.validate(ScheduleSpec.Interval(ScheduleSpec.MinInterval - 1.milli, None)).isLeft)
    val ok = ScheduleSpec.Interval(ScheduleSpec.MinInterval, None)
    assert(ScheduleSpec.validate(ok) == Right(ok))
  }

  // -- Codecs ----------------------------------------------------------------

  test("ScheduleSpec round-trips through its circe codec for every variant") {
    val specs = Seq[ScheduleSpec](
      ScheduleSpec.Hourly(15, "UTC"),
      ScheduleSpec.Daily(LocalTime.of(9, 30), "America/New_York"),
      ScheduleSpec.Daily(LocalTime.of(9, 30, 15), "UTC"), // to the second
      ScheduleSpec.Weekly(DayOfWeek.WEDNESDAY, LocalTime.of(6, 0), "UTC"),
      ScheduleSpec.Monthly(1, LocalTime.MIDNIGHT, "Europe/London"),
      ScheduleSpec.Interval(everyHour, Some(ms(epochHour))),
      ScheduleSpec.Interval(everyHour, None),
    )
    specs.foreach { spec =>
      val json = ScheduleSpec.encoder(spec)
      assert(ScheduleSpec.decoder.decodeJson(json) == Right(spec), s"round-trip for $spec")
    }
  }

  // -- Generic algorithm -----------------------------------------------------

  test("armDelay clamps to [0, cap] and defaults to cap when nothing is scheduled") {
    import scala.concurrent.duration._
    val cap = 5.minutes
    val now = ms(100000)
    // Past deadline floors at 0 (fire immediately).
    assert(JobSchedule.armDelay(Map("a" -> state(next = 50000)), now, cap) == Duration.Zero)
    // Near deadline is exact.
    assert(JobSchedule.armDelay(Map("a" -> state(next = 100000 + 30000)), now, cap) == 30.seconds)
    // Far deadline clamps to the cap.
    assert(JobSchedule.armDelay(Map("a" -> state(next = 100000 + cap.toMillis + 1)), now, cap) == cap)
    // Nothing scheduled (empty, all running, or never-firing) re-checks at the cap.
    assert(JobSchedule.armDelay(Map.empty, now, cap) == cap)
    assert(JobSchedule.armDelay(Map("r" -> state(next = 50, inProgress = Some(5))), now, cap) == cap)
    assert(JobSchedule.armDelay(Map("n" -> state(next = 50).copy(nextFireAt = None)), now, cap) == cap)
  }

  test("armDelay cannot overflow on an extreme deadline (regression: FiniteDuration range)") {
    import scala.concurrent.duration._
    // A deadline of Long.MaxValue minus a small `now` exceeds the ~292-year range FiniteDuration
    // accepts; armDelay must clamp in Long space rather than throw.
    val extreme = Map("far" -> state(next = Long.MaxValue))
    assert(JobSchedule.armDelay(extreme, ms(100), 5.minutes) == 5.minutes)
  }

  test("onCompletion clears the in-progress marker") {
    val done = JobSchedule.onCompletion(state(next = 100, inProgress = Some(10)))
    assert(done.inProgressSince.isEmpty)
  }

  test("interrupted returns jobs left mid-run") {
    val states = Map(
      "idle" -> state(next = 100),
      "running" -> state(next = 100, inProgress = Some(10)),
    )
    assert(JobSchedule.interrupted(states) == Set("running"))
  }
}

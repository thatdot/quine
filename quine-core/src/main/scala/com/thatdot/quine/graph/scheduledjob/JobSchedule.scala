package com.thatdot.quine.graph.scheduledjob

import java.time.{DayOfWeek, Instant, LocalTime, ZoneId}

import scala.concurrent.duration._
import scala.util.Try

import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import io.circe.{Decoder, DecodingFailure, Encoder, Json}

import com.thatdot.quine.model.Milliseconds

/** When a scheduled job fires. A closed set of recurrence shapes: four wall-clock recurrences
  * ([[ScheduleSpec.Hourly]], [[ScheduleSpec.Daily]], [[ScheduleSpec.Weekly]], [[ScheduleSpec.Monthly]])
  * evaluated in an IANA timezone, plus a fixed-[[ScheduleSpec.Interval]] recurrence anchored to an
  * instant. Callers build these structured variants; the wall-clock ones are translated to a
  * Spring-flavored cron expression internally, only at the cron-utils boundary (see
  * [[ScheduleSpec.CronBacked]]).
  */
sealed trait ScheduleSpec {

  /** The next fire time strictly after `from`, or `None` if this schedule never fires again (only
    * reachable from an undecodable persisted spec — the API validates on create). Missed slots
    * collapse to one: N slots missed in downtime fire once, then resume the schedule — slots are
    * not replayed.
    */
  def nextFireAfter(from: Milliseconds): Option[Milliseconds]

  /** The first owed fire time for a job activated at `activatedAt`: the next slot at or on
    * `activatedAt` (a slot exactly at activation is immediately due), or `None` if the schedule
    * never fires.
    */
  def firstFireAt(activatedAt: Milliseconds): Option[Milliseconds]

  /** Resolve any deferred anchor to a concrete instant using `now` when the caller left it unset.
    * Only [[ScheduleSpec.Interval]] has a deferred anchor (its optional `startAt`); every other
    * variant is unchanged. Called once by the scheduler when a job is created so the persisted
    * schedule is fully concrete and its fire times are stable across restarts.
    */
  def anchoredAt(now: Milliseconds): ScheduleSpec = this
}
object ScheduleSpec {

  // Spring cron flavor: six space-separated fields `second minute hour day-of-month month day-of-week`,
  // evaluated to the second. Shared by every wall-clock variant; the cron string is an internal detail
  // built at this boundary and never exposed.
  private val definition = CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING53)
  private val parser = new CronParser(definition)

  /** A wall-clock schedule realized through cron-utils. The variant supplies a Spring-flavored
    * `cronExpression` (built from its structured fields) and the `zoneId` it is evaluated in; this
    * base owns the parse + next-execution machinery. Wall-clock defined, so it is inherently
    * phase-stable: a late or catch-up run advances to the next matching slot rather than drifting.
    *
    * Daylight saving transitions are resolved against the local clock, so the day-, week- and
    * month-grained variants can lose or double a slot. A slot whose local time does not exist on a
    * spring-forward day is skipped rather than shifted — and cron-utils skips the entire clock hour
    * containing the transition, which is wider than the gap in zones whose shift is not a whole hour
    * (`Australia/Lord_Howe`). A slot whose local time occurs twice on a fall-back day yields only the
    * first occurrence, unless the search itself starts inside the repeated hour, in which case the
    * second occurrence is the next slot. [[Hourly]] matches every hour and so simply follows the
    * clock: 23 fires on a spring-forward day, 25 on a fall-back day. The user-facing statement of all
    * this lives on the create-job API endpoint.
    */
  sealed trait CronBacked extends ScheduleSpec {

    /** The Spring cron expression for this recurrence — the only place a cron string exists. */
    protected def cronExpression: String
    def zoneId: String

    // Parsed once (not free), kept off the case-class parameters so it plays no part in
    // equality/serialization. A malformed expression or zone (only reachable from an undecodable
    // persisted blob) yields a schedule that never fires rather than a scheduler-thread throw.
    private lazy val executionTime: Option[ExecutionTime] =
      Try(ExecutionTime.forCron(parser.parse(cronExpression))).toOption
    private lazy val zone: ZoneId = Try(ZoneId.of(zoneId)).getOrElse(ZoneId.of("UTC"))

    /** The next matching instant strictly after `fromMillis`, or `None` if none/invalid. */
    private def nextAfter(fromMillis: Long): Option[Milliseconds] =
      executionTime.flatMap { et =>
        val next = et.nextExecution(Instant.ofEpochMilli(fromMillis).atZone(zone))
        if (next.isPresent) Some(Milliseconds(next.get.toInstant.toEpochMilli)) else None
      }

    override def nextFireAfter(from: Milliseconds): Option[Milliseconds] = nextAfter(from.millis)

    // At-or-on activation: back up 1ms so a slot exactly at `activatedAt` counts as the first fire
    // (cron-utils' nextExecution is strictly after its argument).
    override def firstFireAt(activatedAt: Milliseconds): Option[Milliseconds] = nextAfter(activatedAt.millis - 1)
  }

  /** Fires once an hour at `minute`, in `zoneId`. */
  final case class Hourly(minute: Int, zoneId: String) extends CronBacked {
    protected def cronExpression: String = s"0 $minute * * * *"
  }

  /** Fires once a day at `at` (to the second), in `zoneId`. */
  final case class Daily(at: LocalTime, zoneId: String) extends CronBacked {
    protected def cronExpression: String = s"${at.getSecond} ${at.getMinute} ${at.getHour} * * *"
  }

  /** Fires once a week on `dayOfWeek` at `at` (to the second), in `zoneId`. */
  final case class Weekly(dayOfWeek: DayOfWeek, at: LocalTime, zoneId: String) extends CronBacked {
    protected def cronExpression: String =
      s"${at.getSecond} ${at.getMinute} ${at.getHour} * * ${cronDayOfWeek(dayOfWeek)}"
  }

  /** Fires once a month on `dayOfMonth` at `at` (to the second), in `zoneId`. Months without the day
    * are skipped (cron-utils semantics).
    */
  final case class Monthly(dayOfMonth: Int, at: LocalTime, zoneId: String) extends CronBacked {
    protected def cronExpression: String = s"${at.getSecond} ${at.getMinute} ${at.getHour} $dayOfMonth * *"
  }

  /** Fires at `startAt + k·every` (`k ≥ 0`) — a fixed cadence anchored to an instant, so no
    * timezone or cron, and no daylight saving transition to skip or repeat a slot (the cadence
    * instead drifts against the local clock by the size of the transition). An omitted `startAt` is
    * filled with the job's creation time by
    * [[anchoredAt]], making the interval fire immediately and then every `every`; a future anchor
    * waits, a past one collapses missed slots to the next multiple.
    */
  final case class Interval(every: FiniteDuration, startAt: Option[Milliseconds]) extends ScheduleSpec {

    override def anchoredAt(now: Milliseconds): ScheduleSpec =
      if (startAt.isDefined) this else copy(startAt = Some(now))

    override def nextFireAfter(from: Milliseconds): Option[Milliseconds] = fireAtOrAfter(from.millis + 1)

    override def firstFireAt(activatedAt: Milliseconds): Option[Milliseconds] = fireAtOrAfter(activatedAt.millis)

    /** The smallest `anchor + k·every` (`k ≥ 0`) at or after `target`. Before anchoring the anchor is
      * `target` itself (fire now); a non-positive `every` (only from an undecodable blob, since the API
      * validates) never fires.
      */
    private def fireAtOrAfter(target: Long): Option[Milliseconds] = {
      val anchor = startAt.map(_.millis).getOrElse(target)
      val step = every.toMillis
      if (step <= 0) None
      else if (target <= anchor) Some(Milliseconds(anchor))
      else {
        val k = (target - anchor + step - 1) / step // ceil division
        Some(Milliseconds(anchor + k * step))
      }
    }
  }

  private def cronDayOfWeek(d: DayOfWeek): String = d match {
    case DayOfWeek.MONDAY => "MON"
    case DayOfWeek.TUESDAY => "TUE"
    case DayOfWeek.WEDNESDAY => "WED"
    case DayOfWeek.THURSDAY => "THU"
    case DayOfWeek.FRIDAY => "FRI"
    case DayOfWeek.SATURDAY => "SAT"
    case DayOfWeek.SUNDAY => "SUN"
  }

  private def checkZone(zoneId: String): Either[String, Unit] =
    Try(ZoneId.of(zoneId)).toEither.left.map(_ => s"Invalid timezone: $zoneId").map(_ => ())

  private def inRange(name: String, value: Int, lo: Int, hi: Int): Either[String, Unit] =
    if (value >= lo && value <= hi) Right(()) else Left(s"$name must be between $lo and $hi, got $value")

  /** The shortest [[Interval]] cadence a job may be created with. Fire rate is what sets the
    * steady-state cost of a job — each fire mints an execution status record, and records
    * accumulate at fire rate × retention — so the floor is deliberately coarse: sub-hour cadences
    * are the province of standing queries and ingest, not scheduled jobs.
    */
  val MinInterval: FiniteDuration = 1.hour

  /** The single validation gate, called at the API before a job is created: `minute`/`dayOfMonth`
    * ranges and timezone for the wall-clock variants (`at` is a `LocalTime`, range-safe by
    * construction), and an `every` of at least [[MinInterval]] for [[Interval]]. Returns the same
    * spec, or a human-readable error.
    */
  def validate(spec: ScheduleSpec): Either[String, ScheduleSpec] = spec match {
    case s @ Hourly(minute, zoneId) =>
      for { _ <- inRange("minute", minute, 0, 59); _ <- checkZone(zoneId) } yield s
    case s @ Daily(_, zoneId) => checkZone(zoneId).map(_ => s)
    case s @ Weekly(_, _, zoneId) => checkZone(zoneId).map(_ => s)
    case s @ Monthly(dayOfMonth, _, zoneId) =>
      for { _ <- inRange("dayOfMonth", dayOfMonth, 1, 31); _ <- checkZone(zoneId) } yield s
    case s @ Interval(every, _) =>
      if (every >= MinInterval) Right(s)
      else Left(s"Interval 'every' must be at least ${MinInterval.toHours} hour(s)")
  }

  private def parseLocalTime(s: String, c: io.circe.HCursor): Either[DecodingFailure, LocalTime] =
    Try(LocalTime.parse(s)).toEither.left.map(_ => DecodingFailure(s"Invalid time of day: $s", c.history))

  // Hand-written (quine-core has circe-core only, no generic derivation). A "type" discriminator
  // keeps the JSON open to future variants; times of day are ISO-8601 strings, durations/anchors
  // are millis — API-layer wire forms are the API's concern.
  implicit val encoder: Encoder[ScheduleSpec] = Encoder.instance {
    case Hourly(minute, zoneId) =>
      Json.obj(
        "type" -> Json.fromString("hourly"),
        "minute" -> Json.fromInt(minute),
        "zoneId" -> Json.fromString(zoneId),
      )
    case Daily(at, zoneId) =>
      Json.obj(
        "type" -> Json.fromString("daily"),
        "at" -> Json.fromString(at.toString),
        "zoneId" -> Json.fromString(zoneId),
      )
    case Weekly(dayOfWeek, at, zoneId) =>
      Json.obj(
        "type" -> Json.fromString("weekly"),
        "dayOfWeek" -> Json.fromString(dayOfWeek.name),
        "at" -> Json.fromString(at.toString),
        "zoneId" -> Json.fromString(zoneId),
      )
    case Monthly(dayOfMonth, at, zoneId) =>
      Json.obj(
        "type" -> Json.fromString("monthly"),
        "dayOfMonth" -> Json.fromInt(dayOfMonth),
        "at" -> Json.fromString(at.toString),
        "zoneId" -> Json.fromString(zoneId),
      )
    case Interval(every, startAt) =>
      Json.obj(
        "type" -> Json.fromString("interval"),
        "everyMillis" -> Json.fromLong(every.toMillis),
        "startAtMillis" -> startAt.map(ms => Json.fromLong(ms.millis)).getOrElse(Json.Null),
      )
  }
  implicit val decoder: Decoder[ScheduleSpec] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case "hourly" =>
        for {
          minute <- c.get[Int]("minute")
          zoneId <- c.get[String]("zoneId")
        } yield Hourly(minute, zoneId)
      case "daily" =>
        for {
          at <- c.get[String]("at").flatMap(parseLocalTime(_, c))
          zoneId <- c.get[String]("zoneId")
        } yield Daily(at, zoneId)
      case "weekly" =>
        for {
          dayOfWeekStr <- c.get[String]("dayOfWeek")
          dayOfWeek <- Try(DayOfWeek.valueOf(dayOfWeekStr)).toEither.left
            .map(_ => DecodingFailure(s"Invalid dayOfWeek: $dayOfWeekStr", c.history))
          at <- c.get[String]("at").flatMap(parseLocalTime(_, c))
          zoneId <- c.get[String]("zoneId")
        } yield Weekly(dayOfWeek, at, zoneId)
      case "monthly" =>
        for {
          dayOfMonth <- c.get[Int]("dayOfMonth")
          at <- c.get[String]("at").flatMap(parseLocalTime(_, c))
          zoneId <- c.get[String]("zoneId")
        } yield Monthly(dayOfMonth, at, zoneId)
      case "interval" =>
        for {
          everyMillis <- c.get[Long]("everyMillis")
          startAtMillis <- c.get[Option[Long]]("startAtMillis")
        } yield Interval(FiniteDuration(everyMillis, MILLISECONDS), startAtMillis.map(Milliseconds(_)))
      case other => Left(DecodingFailure(s"Unknown schedule type: $other", c.history))
    }
  }
}

/** Persisted per-job scheduling state, owned by whichever driver is active (the elected manager in
  * enterprise, the single host in OSS). The job's name is the registry key, not a field here. The
  * scheduler is generic: `jobType` tags the kind of work and `payload` is opaque JSON the
  * app-registered executor interprets — new job types are new tags plus app-side handling, with no
  * scheduler changes.
  *
  * `inProgressSince` marks a run that has been launched but not yet observed to complete — used for
  * the overlap guard and at-least-once re-fire on restart/failover. A job's dispatched executions
  * are discoverable through their status records, which carry the dispatching job's name.
  */
final case class ScheduledJobState(
  jobType: String,
  payload: String,
  schedule: ScheduleSpec,
  nextFireAt: Option[Milliseconds],
  lastFireAt: Option[Milliseconds] = None,
  inProgressSince: Option[Milliseconds] = None,
  // Id of the request that created (or last replaced) this job; used only to make create idempotent
  // under at-least-once retry (see [[ScheduledJobDriver.createJob]]). `None` for jobs persisted
  // before this field existed — decoded from an absent key as `None`, so old blobs load unchanged.
  createRequestId: Option[String] = None,
)
object ScheduledJobState {
  implicit private val msEncoder: Encoder[Milliseconds] = Encoder.encodeLong.contramap(_.millis)
  implicit private val msDecoder: Decoder[Milliseconds] = Decoder.decodeLong.map(Milliseconds(_))

  implicit val encoder: Encoder[ScheduledJobState] =
    Encoder.forProduct7(
      "jobType",
      "payload",
      "schedule",
      "nextFireAt",
      "lastFireAt",
      "inProgressSince",
      "createRequestId",
    )(s => (s.jobType, s.payload, s.schedule, s.nextFireAt, s.lastFireAt, s.inProgressSince, s.createRequestId))
  implicit val decoder: Decoder[ScheduledJobState] =
    Decoder.forProduct7(
      "jobType",
      "payload",
      "schedule",
      "nextFireAt",
      "lastFireAt",
      "inProgressSince",
      "createRequestId",
    )(ScheduledJobState.apply)
}

/** Pure scheduling algorithm, shared by the OSS and enterprise drivers. Every function takes `now`
  * explicitly so it is fully deterministic and unit-testable; the drivers own the timer, persistence,
  * and execution.
  */
object JobSchedule {

  /** Jobs whose next fire time has arrived and are not already running. A job with no next fire
    * time (`nextFireAt = None`, an unvalidated persisted spec) is never due.
    */
  def dueJobs(states: Map[String, ScheduledJobState], now: Milliseconds): Set[String] =
    states.iterator.collect {
      case (id, s) if s.inProgressSince.isEmpty && s.nextFireAt.exists(_.millis <= now.millis) => id
    }.toSet

  /** Earliest upcoming deadline among jobs not currently running — what the one-shot timer arms to. */
  def nextDeadline(states: Map[String, ScheduledJobState]): Option[Milliseconds] =
    states.values.iterator.filter(_.inProgressSince.isEmpty).flatMap(_.nextFireAt).minByOption(_.millis)

  /** Delay to arm the driver's one-shot timer: time until the earliest deadline, clamped to
    * `[0, cap]` — `cap` when nothing is scheduled (the capped wake re-checks and sweeps). Clamping
    * happens in `Long` space *before* a `FiniteDuration` is constructed, because a far-future
    * deadline minus `now` can exceed the ~292-year range `FiniteDuration` accepts and would throw
    * before any `.min` could apply. This function owns that invariant for every driver.
    */
  def armDelay(states: Map[String, ScheduledJobState], now: Milliseconds, cap: FiniteDuration): FiniteDuration =
    nextDeadline(states) match {
      case Some(d) => math.min(math.max(0L, d.millis - now.millis), cap.toMillis).millis
      case None => cap
    }

  /** Transition on firing: mark running, record the fire, advance to the next slot (fixed cadence —
    * the next fire does not wait for this run to finish).
    */
  def onFire(state: ScheduledJobState, now: Milliseconds): ScheduledJobState =
    state.copy(
      inProgressSince = Some(now),
      lastFireAt = Some(now),
      nextFireAt = state.schedule.nextFireAfter(now),
    )

  /** Transition on completion (success or failure): the run is no longer in flight. */
  def onCompletion(state: ScheduledJobState): ScheduledJobState =
    state.copy(inProgressSince = None)

  /** Jobs left mid-run by a crashed/failed-over driver — re-fired on activation for at-least-once. */
  def interrupted(states: Map[String, ScheduledJobState]): Set[String] =
    states.iterator.collect { case (id, s) if s.inProgressSince.isDefined => id }.toSet
}

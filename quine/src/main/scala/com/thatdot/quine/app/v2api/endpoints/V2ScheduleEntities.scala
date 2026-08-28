package com.thatdot.quine.app.v2api.endpoints

import java.time.{DayOfWeek, Instant, LocalTime}

import scala.concurrent.duration.FiniteDuration

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample, title}

import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.{circeConfig, tapirConfig}
import com.thatdot.api.v2.codec.ScreamingSnakeEnum
import com.thatdot.api.v2.codec.ThirdPartyCodecs.jdk.{
  instantDecoder,
  instantEncoder,
  localTimeDecoder,
  localTimeEncoder,
}
import com.thatdot.api.v2.codec.ThirdPartyCodecs.scala.{finiteDurationDecoder, finiteDurationEncoder}
import com.thatdot.api.v2.schema.ThirdPartySchemas.jdk.{instantSchema, localTimeSchema}
import com.thatdot.api.v2.schema.ThirdPartySchemas.scala.finiteDurationSchema
import com.thatdot.quine.graph.scheduledjob.ScheduleSpec
import com.thatdot.quine.model.Milliseconds

/** When a scheduled job fires. A discriminated (`"type"`) sealed trait: four wall-clock recurrences
  * ([[Schedule.Hourly]], [[Schedule.Daily]], [[Schedule.Weekly]], [[Schedule.Monthly]]) evaluated
  * in an IANA `timezone`, plus a fixed-[[Schedule.Interval]] recurrence anchored to an instant. The
  * wall-clock variants therefore inherit their zone's daylight saving transitions (skipped and
  * repeated local times); [[Schedule.Interval]] has no zone and is immune. The user-facing rules are
  * documented on the create-job endpoint. This is the wire shape; [[Schedule.toModel]] validates and
  * maps it to the domain `ScheduleSpec`, and [[Schedule.fromModel]] renders one back for status
  * responses.
  */
sealed trait Schedule {

  /** Validate and convert to the domain schedule, or a human-readable error. */
  def toModel: Either[String, ScheduleSpec] = ScheduleSpec.validate(Schedule.toSpec(this))
}
object Schedule {

  // java.time.DayOfWeek over the wire as an AIP-126 screaming-snake string (MONDAY..SUNDAY). Its
  // toString is already uppercase, so the wire value is the day name verbatim.
  implicit val dayOfWeekEncoder: Encoder[DayOfWeek] = ScreamingSnakeEnum.encoder
  implicit val dayOfWeekDecoder: Decoder[DayOfWeek] = ScreamingSnakeEnum.decoder(DayOfWeek.values.toIndexedSeq)
  implicit lazy val dayOfWeekSchema: Schema[DayOfWeek] = ScreamingSnakeEnum.schema(DayOfWeek.values.toIndexedSeq)

  // Shared wording for the `timezone` field of every wall-clock variant. The DST rules the zone
  // implies are spelled out once, on the create-job endpoint.
  final private val TimezoneDescription =
    "IANA timezone id (e.g. \"America/New_York\"), matched case-sensitively. Fixed-offset ids " +
    "(\"UTC\", \"-05:00\", \"Etc/GMT+5\") never observe daylight saving time; named regional zones do, " +
    "and their transitions can skip or repeat a fire — see the create-job description. Defaults to UTC."

  @title("Hourly")
  @description(
    "Fires once an hour at the given minute, in the given timezone. This tracks the local clock " +
    "across a daylight saving transition: the spring-forward day has 23 fires and the fall-back day " +
    "has 25 (the repeated hour fires twice).",
  )
  final case class Hourly(
    @description("Minute of the hour (0–59).") minute: Int,
    @description(TimezoneDescription) timezone: String = "UTC",
  ) extends Schedule

  @title("Daily")
  @description(
    "Fires once a day at the given time of day, in the given timezone. If that time of day does not " +
    "exist on the spring-forward day, the fire is skipped that day; if it occurs twice on the " +
    "fall-back day, the job fires once, at the first occurrence.",
  )
  final case class Daily(
    @description("Time of day, \"HH:mm\" or \"HH:mm:ss\" (e.g. \"09:30\").") at: LocalTime,
    @description(TimezoneDescription) timezone: String = "UTC",
  ) extends Schedule

  @title("Weekly")
  @description(
    "Fires once a week on the given day at the given time of day, in the given timezone. If that " +
    "time of day does not exist on a spring-forward day that falls on the chosen weekday, that week " +
    "is skipped entirely; a time of day repeated by a fall-back fires once, at the first occurrence.",
  )
  final case class Weekly(
    @description("Day of the week (MONDAY…SUNDAY).") dayOfWeek: DayOfWeek,
    @description("Time of day, \"HH:mm\" or \"HH:mm:ss\" (e.g. \"09:30\").") at: LocalTime,
    @description(TimezoneDescription) timezone: String = "UTC",
  ) extends Schedule

  @title("Monthly")
  @description(
    "Fires once a month on the given day at the given time of day. Months without the day are " +
    "skipped. So is a month whose chosen day is a spring-forward day on which that time of day does " +
    "not exist; a time of day repeated by a fall-back fires once, at the first occurrence.",
  )
  final case class Monthly(
    @description("Day of the month (1–31).") dayOfMonth: Int,
    @description("Time of day, \"HH:mm\" or \"HH:mm:ss\" (e.g. \"09:30\").") at: LocalTime,
    @description(TimezoneDescription) timezone: String = "UTC",
  ) extends Schedule

  @title("Interval")
  @description(
    "Fires at a fixed cadence anchored to an instant: at startAt, then every `every` thereafter. " +
    "If startAt is omitted the anchor resolves to the time the job is created or replaced (so the job " +
    "fires immediately, then every interval); replacing a job re-anchors it — see `updateIfExists`. " +
    "The cadence must be at least one hour. " +
    "Has no timezone: the cadence is measured in absolute time, so it never skips or " +
    "repeats a fire at a daylight saving transition, and instead drifts by the size of the transition " +
    "against the local clock (a 24h interval firing at 09:00 local fires at 10:00 local after a " +
    "spring-forward).",
  )
  final case class Interval(
    @description("The interval between fires, as an AIP-142 duration string (e.g. \"1h30m\", \"24h\"); minimum \"1h\".")
    @encodedExample("24h")
    every: FiniteDuration,
    @description("RFC-3339 anchor instant. Omitted ⇒ resolved to the time the job is created or replaced.")
    startAt: Option[Instant] = None,
  ) extends Schedule

  private def toSpec(t: Schedule): ScheduleSpec = t match {
    case Hourly(minute, timezone) => ScheduleSpec.Hourly(minute, timezone)
    case Daily(at, timezone) => ScheduleSpec.Daily(at, timezone)
    case Weekly(dayOfWeek, at, timezone) => ScheduleSpec.Weekly(dayOfWeek, at, timezone)
    case Monthly(dayOfMonth, at, timezone) => ScheduleSpec.Monthly(dayOfMonth, at, timezone)
    case Interval(every, startAt) => ScheduleSpec.Interval(every, startAt.map(i => Milliseconds(i.toEpochMilli)))
  }

  /** Render a (concrete) domain schedule back to its wire form for status responses. */
  def fromModel(spec: ScheduleSpec): Schedule = spec match {
    case ScheduleSpec.Hourly(minute, zoneId) => Hourly(minute, zoneId)
    case ScheduleSpec.Daily(at, zoneId) => Daily(at, zoneId)
    case ScheduleSpec.Weekly(dayOfWeek, at, zoneId) => Weekly(dayOfWeek, at, zoneId)
    case ScheduleSpec.Monthly(dayOfMonth, at, zoneId) => Monthly(dayOfMonth, at, zoneId)
    case ScheduleSpec.Interval(every, startAt) => Interval(every, startAt.map(ms => Instant.ofEpochMilli(ms.millis)))
  }

  implicit val encoder: Encoder[Schedule] = deriveConfiguredEncoder
  implicit val decoder: Decoder[Schedule] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[Schedule] = Schema.derived
}

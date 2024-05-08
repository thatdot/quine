package com.thatdot.quine.app.serialization
import java.net.URL
import java.time.{LocalDateTime, ZoneOffset}

import scala.jdk.CollectionConverters._

import cats.data.{Chain, NonEmptyChain}
import cats.implicits._
import com.google.`type`.{Date, DateTime, TimeOfDay}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.Descriptors.{Descriptor, EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.{ByteString, Duration, DynamicMessage, Timestamp}

import com.thatdot.quine.model.{QuineType, QuineValue}

// TODO: at pretty string representations of these errors.
sealed abstract class ConversionFailure
final case class TypeMismatch(provided: QuineType, expected: JavaType) extends ConversionFailure {
  def message: String = s"Can't coerce $provided into $expected"
}
final case class UnexpectedNull(fieldName: String) extends ConversionFailure {
  def message: String = s"Unexpected null for field '$fieldName'"
}
case object NotAList extends ConversionFailure
final case class InvalidEnumValue(provided: String, expected: Seq[EnumValueDescriptor]) extends ConversionFailure
final case class FieldError(fieldName: String, conversionFailure: ConversionFailure) extends ConversionFailure {
  //override def message: String = s"Error converting field '$fieldName': $conversionFailure"
}
final case class ErrorCollection(errors: NonEmptyChain[ConversionFailure]) extends ConversionFailure

/** Converts QuineValues to Protobuf messages according to a schema.
  * @throws UnreachableProtobufSchema If opening the schema file fails
  * @throws InvalidProtobufSchemaFile If the schema file is invalid
  * @throws NoSuchMessageType         If the schema file does not contain the specified type
  * @throws AmbiguousMessageType      If the schema file contains multiple message descriptors that
  *                                   all match the provided name
  */
class QuineValueToProtobuf(schemaUrl: URL, typeName: String) extends ProtobufSchema(schemaUrl, typeName) {

  /** Mainly for testing
    * @param quineValues
    * @return
    */
  def toProtobuf(quineValues: Map[String, QuineValue]): Either[ConversionFailure, DynamicMessage] =
    mapToProtobuf(messageType, quineValues)

  def toProtobufBytes(quineValues: Map[String, QuineValue]): Either[ConversionFailure, Array[Byte]] =
    toProtobuf(quineValues).map(_.toByteArray)

  def mapToProtobuf(descriptor: Descriptor, map: Map[String, QuineValue]): Either[ConversionFailure, DynamicMessage] = {
    val builder = DynamicMessage.newBuilder(descriptor)
    val protbufFields = descriptor.getFields.asScala.view
    var errors = Chain.empty[ConversionFailure]
    for {
      field <- protbufFields
      // Nulls get skipped.
      quineValue <- map.get(field.getName) if quineValue != QuineValue.Null
    } quineValueToProtobuf(field, quineValue) match {
      case Right(value) => builder.setField(field, value)
      case Left(err) => errors = errors.append(FieldError(field.getName, err))
    }
    NonEmptyChain.fromChain(errors) match {
      case Some(nec) => Left(ErrorCollection(nec))
      case None => Right(builder.build)
    }
  }

  private def dateTimeToProtobuf(datetime: LocalDateTime): DateTime.Builder = DateTime.newBuilder
    .setYear(datetime.getYear)
    .setMonth(datetime.getMonthValue)
    .setDay(datetime.getDayOfMonth)
    .setHours(datetime.getHour)
    .setMinutes(datetime.getMinute)
    .setSeconds(datetime.getSecond)
    .setNanos(datetime.getNano)

  @throws[IllegalArgumentException]("If the value provided is Null")
  def quineValueToProtobuf(field: FieldDescriptor, qv: QuineValue): Either[ConversionFailure, AnyRef] = qv match {
    case QuineValue.Str(string) =>
      field.getJavaType match {
        case JavaType.STRING => Right(string)
        case JavaType.ENUM =>
          val pbEnum = field.getEnumType
          Option(pbEnum.findValueByName(string)) toRight InvalidEnumValue(string, pbEnum.getValues.asScala.toVector)
        case other => Left(TypeMismatch(qv.quineType, other))
      }
    case QuineValue.Integer(long) =>
      field.getJavaType match {
        case JavaType.LONG => Right(Long.box(long))
        case JavaType.INT => Right(Int.box(long.toInt))
        case other => Left(TypeMismatch(qv.quineType, other))
      }
    case QuineValue.Floating(double) =>
      field.getJavaType match {
        case JavaType.DOUBLE => Right(Double.box(double))
        case JavaType.FLOAT => Right(Float.box(double.toFloat))
        case other => Left(TypeMismatch(qv.quineType, other))
      }
    case QuineValue.True =>
      Either.cond(
        field.getJavaType == JavaType.BOOLEAN,
        java.lang.Boolean.TRUE,
        TypeMismatch(QuineType.Boolean, field.getJavaType)
      )
    case QuineValue.False =>
      Either.cond(
        field.getJavaType == JavaType.BOOLEAN,
        java.lang.Boolean.FALSE,
        TypeMismatch(QuineType.Boolean, field.getJavaType)
      )
    case QuineValue.Null => Left(UnexpectedNull(field.getName))
    case QuineValue.Bytes(bytes) =>
      Either.cond(
        field.getJavaType == JavaType.BYTE_STRING,
        ByteString.copyFrom(bytes),
        TypeMismatch(QuineType.Bytes, field.getJavaType)
      )
    case QuineValue.List(list) =>
      if (field.isRepeated)
        list.parTraverse(v => quineValueToProtobuf(field, v).toEitherNec).bimap(ErrorCollection, _.asJava)
      else
        Left(NotAList)

    case QuineValue.Map(map) =>
      if (field.getJavaType == JavaType.MESSAGE)
        mapToProtobuf(field.getMessageType, map)
      else
        Left(TypeMismatch(QuineType.Map, field.getJavaType))
    case QuineValue.DateTime(datetime) =>
      field.getJavaType match {
        case JavaType.LONG => Right(Long.box(datetime.toInstant.toEpochMilli))
        case JavaType.STRING => Right(datetime.toString)
        case JavaType.MESSAGE =>
          val targetMessageType = field.getMessageType
          if (targetMessageType == Timestamp.getDescriptor) {
            val builder = Timestamp.newBuilder
            val instant = datetime.toInstant
            builder.setSeconds(instant.getEpochSecond)
            builder.setNanos(instant.getNano)
            Right(builder.build)
          } else if (targetMessageType == DateTime.getDescriptor) {
            Right(
              dateTimeToProtobuf(datetime.toLocalDateTime)
                .setUtcOffset(Duration.newBuilder.setSeconds(datetime.getOffset.getTotalSeconds.toLong))
                .build
            )
            // TODO: Give a more specific error message that says:
            // "Yes, it's a message, but not the right type of message."
            // The current error message will say "Can't coerce DateTime to MESSAGE",
            // but that should give you a clue of what's going on anyways.
          } else Left(TypeMismatch(QuineType.DateTime, JavaType.MESSAGE))
        case other => Left(TypeMismatch(qv.quineType, other))
      }
    case QuineValue.Duration(javaDuration) =>
      field.getJavaType match {
        case JavaType.LONG => Right(Long.box(javaDuration.toMillis))
        case JavaType.STRING => Right(javaDuration.toString)
        // TODO: Move this `if the message type matches the Timestamp schema out of the pattern-match
        case JavaType.MESSAGE if field.getMessageType == Duration.getDescriptor =>
          val builder = Duration.newBuilder
          builder.setSeconds(javaDuration.getSeconds)
          builder.setNanos(javaDuration.getNano)
          Right(builder.build)
        case other => Left(TypeMismatch(qv.quineType, other))
      }

    case QuineValue.Date(localDate) =>
      field.getJavaType match {
        case JavaType.LONG => Right(Long.box(localDate.toEpochDay))
        case JavaType.STRING => Right(localDate.toString)
        // TODO: Move this `if the message type matches the Timestamp schema out of the pattern-match
        case JavaType.MESSAGE if field.getMessageType == Date.getDescriptor =>
          val builder = Date.newBuilder
          builder.setDay(localDate.getDayOfMonth)
          builder.setMonth(localDate.getMonthValue)
          //TODO Protobuf lib. Only supports positive years 1-9999, while javaLocalDate supports -999999999 to 999999999
          builder.setYear(localDate.getYear)
          Right(builder.build)
        case other => Left(TypeMismatch(qv.quineType, other))
      }
    case QuineValue.Time(time) =>
      field.getJavaType match {
        // Do we just not support writing times into Longs, or normalize them to UTC, or write them as local times?
        // case JavaType.LONG => Right(Long.box(time.withOffsetSameInstant(ZoneOffset.UTC).toLocalTime toNanoOfDay))
        case JavaType.STRING => Right(time.toString)
        // TODO: Move this `if the message type matches the Timestamp schema out of the pattern-match
        // Same question here as for long above: This TimeOfDay doesn't store offset
        // Do we use local time, normalize to UTC, or just not support it?
        case JavaType.MESSAGE if field.getMessageType == TimeOfDay.getDescriptor =>
          val builder = TimeOfDay.newBuilder
          builder.setHours(time.getHour)
          builder.setMinutes(time.getMinute)
          builder.setSeconds(time.getSecond)
          builder.setNanos(time.getNano)
          Right(builder.build)
        case other => Left(TypeMismatch(qv.quineType, other))
      }
    case QuineValue.LocalTime(localTime) =>
      field.getJavaType match {
        case JavaType.LONG => Right(Long.box(localTime.toNanoOfDay))
        case JavaType.STRING => Right(localTime.toString)
        // TODO: Move this `if the message type matches the Timestamp schema out of the pattern-match
        case JavaType.MESSAGE if field.getMessageType == TimeOfDay.getDescriptor =>
          val builder = TimeOfDay.newBuilder
          builder.setHours(localTime.getHour)
          builder.setMinutes(localTime.getMinute)
          builder.setSeconds(localTime.getSecond)
          builder.setNanos(localTime.getNano)
          Right(builder.build)
        case other => Left(TypeMismatch(qv.quineType, other))
      }
    case QuineValue.LocalDateTime(ldt) =>
      field.getJavaType match {
        case JavaType.LONG => Right(Long.box(ldt.toInstant(ZoneOffset.UTC).toEpochMilli))
        case JavaType.STRING => Right(ldt.toString)
        // TODO: Move this `if the message type matches the Timestamp schema out of the pattern-match
        case JavaType.MESSAGE if field.getMessageType == DateTime.getDescriptor =>
          Right(
            dateTimeToProtobuf(ldt).build
          )
        case other => Left(TypeMismatch(qv.quineType, other))
      }
    // TODO: add String by going through qidToPrettyString
    // Try applying qidToValue and then seeing if the output value fits in the requested schema type
    case QuineValue.Id(id) =>
      Either.cond(
        field.getJavaType == JavaType.BYTE_STRING,
        ByteString.copyFrom(id.array),
        TypeMismatch(QuineType.Id, field.getJavaType)
      )
  }

}

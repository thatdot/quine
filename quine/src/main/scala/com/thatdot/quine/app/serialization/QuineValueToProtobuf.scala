package com.thatdot.quine.app.serialization

import java.lang.Boolean
import java.net.URL

import scala.jdk.CollectionConverters._

import cats.data.{Chain, NonEmptyChain}
import cats.implicits._
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.Descriptors.{Descriptor, EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.{ByteString, DynamicMessage, Timestamp}

import com.thatdot.quine.model.{QuineType, QuineValue}

// TODO: at pretty string representations of these errors.
sealed abstract class ConversionFailure
final case class TypeMismatch(given: QuineType, expected: JavaType) extends ConversionFailure {
  def message: String = s"Can't coerce $given into $expected"
}
case object NotAList extends ConversionFailure
final case class InvalidEnumValue(given: String, expected: Seq[EnumValueDescriptor]) extends ConversionFailure
final case class FieldError(fieldName: String, conversionFailure: ConversionFailure) extends ConversionFailure {
  //override def message: String = s"Error converting field '$fieldName': $conversionFailure"
}
final case class ErrorCollection(errors: NonEmptyChain[ConversionFailure]) extends ConversionFailure

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

  def quineValueToProtobuf(field: FieldDescriptor, qv: QuineValue): Either[ConversionFailure, AnyRef] = qv match {
    case QuineValue.Str(string) =>
      field.getJavaType match {
        case JavaType.STRING => Right(string)
        case JavaType.ENUM =>
          val enum = field.getEnumType
          Option(enum.findValueByName(string)) toRight InvalidEnumValue(string, enum.getValues.asScala)
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
        Boolean.TRUE,
        TypeMismatch(QuineType.Boolean, field.getJavaType)
      )
    case QuineValue.False =>
      Either.cond(
        field.getJavaType == JavaType.BOOLEAN,
        Boolean.FALSE,
        TypeMismatch(QuineType.Boolean, field.getJavaType)
      )
    // Just so the linting doesn't complain about missing cases:
    case QuineValue.Null => sys.error("This case should not happen because we filter out nulls.")
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
    case QuineValue.DateTime(time) =>
      field.getJavaType match {
        case JavaType.LONG => Right(Long.box(time.toEpochMilli))
        case JavaType.STRING => Right(time.toString)
        // TODO: Move this `if the message type matches the Timestamp schema out of the pattern-match
        // to be able to give an error message that says "Yes, it's a message, but not the right type of message."
        // The current error message will say "Can't coerce DateTime to MESSAGE" in that case,
        // but that should give you a clue of what's going on anyways.
        case JavaType.MESSAGE if field.getMessageType == Timestamp.getDescriptor =>
          val builder = Timestamp.newBuilder
          builder.setSeconds(time.getEpochSecond)
          builder.setNanos(time.getNano)
          Right(builder.build)
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

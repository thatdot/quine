package com.thatdot.quine.app.ingest.serialization

import java.net.URL

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

import com.google.protobuf.Descriptors.EnumValueDescriptor
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.google.protobuf.{ByteString, DynamicMessage}

import com.thatdot.quine.app.serialization.ProtobufSchema
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.Expr

class ProtobufParser(schemaUrl: URL, typeName: String) extends ProtobufSchema(schemaUrl, typeName) {

  def parseBytes(bytes: Array[Byte]): cypher.Value =
    protobufMessageToCypher(
      DynamicMessage.parseFrom(messageType, bytes)
    )

  private def protobufMessageToCypher(message: DynamicMessage): cypher.Value = Expr.Map {
    val descriptor = message.getDescriptorForType
    val oneOfs = descriptor.getOneofs.asScala.view
    // optionals are modeled as (synthetic) oneOfs of a single field.
    val (optionals, realOneOfs) = oneOfs.partition(_.isSynthetic)
    // synthetic oneOfs (optionals) just have the one field
    val setOptionals = optionals.map(_.getField(0)).filter(message.hasField)
    // Find which field in each oneOf is set
    val oneOfFields = realOneOfs.flatMap(oneOf => oneOf.getFields.asScala.find(message.hasField))
    val regularFields = descriptor.getFields.asScala.view diff oneOfs.flatMap(_.getFields.asScala)
    (setOptionals ++ oneOfFields ++ regularFields)
      .map(field =>
        (
          field.getName,
          if (field.isRepeated) {
            if (field.isMapField)
              Expr.Map(
                message
                  .getField(field)
                  .asInstanceOf[java.util.List[DynamicMessage]]
                  .asScala
                  .map { mapEntry =>
                    /*
                      mapEntry.getDescriptorForType is a type descriped as:
                      message MapFieldEntry {
                        key_type key = 1;
                        value_type value = 2;
                      }
                      We already know what fields it contains.
                     */
                    val mutable.Buffer(k, v) = mapEntry.getDescriptorForType.getFields.asScala
                    assert(k.getName == "key")
                    assert(v.getName == "value")
                    val key = k.getJavaType match {
                      // According to Protobuf docs, "the key_type can be any integral or string type"
                      // https://developers.google.com/protocol-buffers/docs/proto3#maps
                      case STRING => mapEntry.getField(k).asInstanceOf[String]
                      case INT | LONG => mapEntry.getField(k).toString
                      case other =>
                        throw new AssertionError(
                          "Protobuf map keys should be either string or integral; got " + other
                        )
                    }
                    key -> fieldToCypher(v.getJavaType, mapEntry.getField(v))
                  }
                  .toMap
              )
            else
              Expr.List(
                message
                  .getField(field)
                  .asInstanceOf[java.util.List[AnyRef]]
                  .asScala
                  .map(fieldToCypher(field.getJavaType, _))
                  .toVector
              )
          } else {
            fieldToCypher(field.getJavaType, message.getField(field))
          }
        )
      )
      .toMap
  }

  private def fieldToCypher(javaType: JavaType, value: AnyRef): cypher.Value = javaType match {
    case STRING => Expr.Str(value.asInstanceOf[String])
    case INT | LONG => Expr.Integer(value.asInstanceOf[java.lang.Number].longValue)
    case FLOAT | DOUBLE => Expr.Floating(value.asInstanceOf[java.lang.Number].doubleValue)
    case BOOLEAN => Expr.Bool(value.asInstanceOf[java.lang.Boolean])
    case BYTE_STRING => Expr.Bytes(value.asInstanceOf[ByteString].toByteArray)
    case ENUM => Expr.Str(value.asInstanceOf[EnumValueDescriptor].getName)
    case MESSAGE => protobufMessageToCypher(value.asInstanceOf[DynamicMessage])
  }
}

class Protobuf(query: String, parameter: String, schemaUrl: URL, typeName: String)
    extends CypherImportFormat(query, parameter) {

  private val parser = new ProtobufParser(schemaUrl, typeName)

  override protected def importBytes(data: Array[Byte]): Try[cypher.Value] = Try(parser.parseBytes(data))
}

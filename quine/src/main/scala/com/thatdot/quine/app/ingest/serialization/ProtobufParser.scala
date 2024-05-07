package com.thatdot.quine.app.ingest.serialization

import java.net.URL

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.google.protobuf.Descriptors.{DescriptorValidationException, EnumValueDescriptor}
import com.google.protobuf.LegacyDescriptorsUtil.LegacyOneofDescriptor
import com.google.protobuf.{ByteString, Descriptors, DynamicMessage}

import com.thatdot.quine.app.serialization.ProtobufSchema
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.util.QuineDispatchers

/** Parses Protobuf messages to cypher values according to a schema.
  * @throws java.io.IOException If opening the schema file fails
  * @throws DescriptorValidationException If the schema file is invalid
  * @throws IllegalArgumentException If the schema file does not contain the specified type
  */
class ProtobufParser private (schemaUrl: URL, typeName: String) extends ProtobufSchema(schemaUrl, typeName) {

  def parseBytes(bytes: Array[Byte]): Expr.Map =
    protobufMessageToCypher(
      DynamicMessage.parseFrom(messageType, bytes)
    )

  private def protobufMessageToCypher(message: DynamicMessage): Expr.Map = Expr.Map {
    val descriptor = message.getDescriptorForType
    val oneOfs = descriptor.getOneofs.asScala.view
    // optionals are modeled as (synthetic) oneOfs of a single field.
    val (optionals, realOneOfs) = oneOfs.partition(LegacyOneofDescriptor.isSynthetic)
    // synthetic oneOfs (optionals) just have the one field
    val setOptionals = optionals.map(_.getField(0)).filter(message.hasField)
    // Find which field in each oneOf is set
    val oneOfFields = realOneOfs.flatMap(oneOf => oneOf.getFields.asScala.find(message.hasField))
    val regularFields = descriptor.getFields.asScala.view diff oneOfs.flatMap(_.getFields.asScala).toVector
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
                      mapEntry.getDescriptorForType is a type described as:
                      message MapFieldEntry {
                        key_type key = 1;
                        value_type value = 2;
                      }
                      We already know what fields it contains.
                     */
                    val buffer: mutable.Buffer[Descriptors.FieldDescriptor] =
                      mapEntry.getDescriptorForType.getFields.asScala
                    assert(buffer.length == 2)
                    val k = buffer.head
                    val v = buffer.tail.head
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
object ProtobufParser {
  trait Cache {
    def get(schemaUrl: URL, typeName: String): ProtobufParser
    def getFuture(schemaUrl: URL, typeName: String): Future[ProtobufParser]
  }
  object BlockingWithoutCaching extends Cache {
    def get(schemaUrl: URL, typeName: String): ProtobufParser = new ProtobufParser(schemaUrl, typeName)
    def getFuture(schemaUrl: URL, typeName: String): Future[ProtobufParser] =
      Future.successful(new ProtobufParser(schemaUrl, typeName))
  }
  class LoadingCache(val dispatchers: QuineDispatchers) extends Cache {
    import LoadingCache.CacheKey

    // On cache capacity:
    // intelliJ's memory parser computes each entry at around 10KB (order-of-magnitude estimate)
    // TODO QU-1868 we actually want to cache the parsed descriptor file, not the parser generated from a part of
    //   that descriptor.
    private val parserCache: AsyncLoadingCache[CacheKey, ProtobufParser] =
      Scaffeine()
        .maximumSize(800) // 800* 10KB = 8MB maximum heap impact (approx.)
        .buildAsyncFuture { case CacheKey(schemaUrl, typeName) =>
          Future(new ProtobufParser(schemaUrl, typeName))(dispatchers.blockingDispatcherEC)
        }

    /** Get a parser for the given schema and type name. This method will block until the parser is available.
      * see TODO on [[ProtobufSchema]] about blocking at construction time
      */
    @throws[java.io.IOException]("If opening the schema file fails")
    @throws[DescriptorValidationException]("If the schema file is invalid")
    @throws[IllegalArgumentException]("If the schema file does not contain the specified type")
    def get(schemaUrl: URL, typeName: String): ProtobufParser =
      Await.result(parserCache.get(CacheKey(schemaUrl, typeName)), 2.seconds)

    /** Like [[get]], but doesn't block the calling thread, and failures are returned as a Future.failed
      */
    def getFuture(schemaUrl: URL, typeName: String): Future[ProtobufParser] =
      parserCache.get(CacheKey(schemaUrl, typeName))
  }
  object LoadingCache {
    private case class CacheKey(schemaUrl: URL, typeName: String)
  }
}

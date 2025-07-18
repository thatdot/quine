package com.thatdot.data

import scala.collection.{SeqView, View, mutable}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.pekko.util

import com.google.protobuf.Descriptors.EnumValueDescriptor
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.{ByteString, Descriptors, DynamicMessage}
import io.circe.{Json, JsonNumber, JsonObject}
import org.apache.avro.generic.{GenericArray, GenericEnumSymbol, GenericFixed, GenericRecord}

import com.thatdot.common.logging.Log._

trait DataFoldableFrom[A] extends LazySafeLogging {
  def fold[B](value: A, folder: DataFolderTo[B]): B

  def fold[B, Frame](t: (() => Try[A], Frame), folder: DataFolderTo[B]): (Try[B], Frame) =
    (t._1().map(a => fold(a, folder)), t._2)

  def to[B: DataFolderTo: ClassTag]: A => B = {
    case b: B => b
    case a => fold(a, DataFolderTo[B])
  }
}

object DataFoldableFrom {
  def apply[A](implicit df: DataFoldableFrom[A]): DataFoldableFrom[A] = df

  def contramap[A: DataFoldableFrom, B](f: B => A): DataFoldableFrom[B] =
    new DataFoldableFrom[B] {
      override def fold[C](value: B, folder: DataFolderTo[C]): C =
        DataFoldableFrom[A].fold(f(value), folder)
    }

  implicit final class Ops[A](private val self: DataFoldableFrom[A]) extends AnyVal {
    def contramap[B](f: B => A): DataFoldableFrom[B] =
      DataFoldableFrom.contramap(f)(self)
  }

  implicit val jsonDataFoldable: DataFoldableFrom[Json] = new DataFoldableFrom[Json] {
    def fold[B](value: Json, folder: DataFolderTo[B]): B =
      value.foldWith(new Json.Folder[B] {
        def onNull: B = folder.nullValue

        def onBoolean(value: Boolean): B = if (value) folder.trueValue else folder.falseValue

        def onNumber(value: JsonNumber): B =
          value.toLong.fold(folder.floating(value.toDouble))(l => folder.integer(l))

        def onString(value: String): B = folder.string(value)

        def onArray(value: Vector[Json]): B = {
          val builder = folder.vectorBuilder()
          value.foreach(j => builder.add(fold[B](j, folder)))
          builder.finish()
        }

        def onObject(value: JsonObject): B = {
          val builder = folder.mapBuilder()
          value.toIterable.foreach { case (k, v) => builder.add(k, fold[B](v, folder)) }
          builder.finish()
        }
      })
  }

  implicit val byteStringDataFoldable: DataFoldableFrom[util.ByteString] = new DataFoldableFrom[util.ByteString] {
    def fold[B](value: util.ByteString, folder: DataFolderTo[B]): B =
      folder.bytes(value.toArrayUnsafe())
  }

  implicit val bytesDataFoldable: DataFoldableFrom[Array[Byte]] = new DataFoldableFrom[Array[Byte]] {
    def fold[B](value: Array[Byte], folder: DataFolderTo[B]): B =
      folder.bytes(value)
  }

  implicit val stringDataFoldable: DataFoldableFrom[String] = new DataFoldableFrom[String] {
    def fold[B](value: String, folder: DataFolderTo[B]): B =
      folder.string(value)
  }

  implicit val stringIterableDataFoldable: DataFoldableFrom[Iterable[String]] = new DataFoldableFrom[Iterable[String]] {
    override def fold[B](value: Iterable[String], folder: DataFolderTo[B]): B = {
      val builder = folder.vectorBuilder()
      value.foreach(v => builder.add(folder.string(v)))
      builder.finish()
    }
  }

  implicit val stringMapDataFoldable: DataFoldableFrom[Map[String, String]] =
    new DataFoldableFrom[Map[String, String]] {
      override def fold[B](value: Map[String, String], folder: DataFolderTo[B]): B = {
        val builder = folder.mapBuilder()
        value.foreach { case (name, value) =>
          builder.add(name, folder.string(value))
        }
        builder.finish()
      }
    }

  implicit val protobufDataFoldable: DataFoldableFrom[DynamicMessage] = new DataFoldableFrom[DynamicMessage] {
    import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._

    private def fieldToValue[B](javaType: JavaType, value: AnyRef, folder: DataFolderTo[B]): B =
      javaType match {
        case STRING => folder.string(value.asInstanceOf[String])
        case INT | LONG => folder.integer(value.asInstanceOf[java.lang.Number].longValue)
        case FLOAT | DOUBLE => folder.floating(value.asInstanceOf[java.lang.Number].doubleValue)
        case BOOLEAN =>
          val bool = value.asInstanceOf[java.lang.Boolean]
          if (bool) folder.trueValue else folder.falseValue

        case BYTE_STRING => folder.bytes(value.asInstanceOf[ByteString].toByteArray)
        case ENUM => folder.string(value.asInstanceOf[EnumValueDescriptor].getName)
        case MESSAGE => fold(value.asInstanceOf[DynamicMessage], folder)
      }

    override def fold[B](message: DynamicMessage, folder: DataFolderTo[B]): B = {
      val descriptor: Descriptors.Descriptor = message.getDescriptorForType
      val oneOfs: SeqView[Descriptors.OneofDescriptor] = descriptor.getOneofs.asScala.view
      // optionals are modeled as (synthetic) oneOfs of a single field.

      //  Kind of annoying finding a replacement for isSynthetic: https://github.com/googleapis/sdk-platform-java/pull/2764
      val (optionals, realOneOfs) = oneOfs.partition { oneof =>
        // `getRealContainingOneof` call ends up being `null` if the `oneof` is synthetic,
        // with a use of `isSynthetic` in its implementation.
        // There might be a case where a user really has a `oneof` with a single optional
        // field, so I did not use isOptional here.
        oneof.getField(0).getRealContainingOneof == null
      }

      // synthetic oneOfs (optionals) just have the one field
      val setOptionals: View[Descriptors.FieldDescriptor] = optionals.map(_.getField(0)).filter(message.hasField)
      // Find which field in each oneOf is set
      val oneOfFields: View[Descriptors.FieldDescriptor] =
        realOneOfs.flatMap(_.getFields.asScala.find(message.hasField))
      val regularFields = descriptor.getFields.asScala.view diff oneOfs.flatMap(_.getFields.asScala).toVector
      val mapBuilder: DataFolderTo.MapBuilder[B] = folder.mapBuilder()
      (setOptionals ++ oneOfFields ++ regularFields).foreach { field =>

        val b: B = {
          if (field.isRepeated) {
            if (field.isMapField) {

              val localMapBuilder = folder.mapBuilder()

              message
                .getField(field)
                .asInstanceOf[java.util.List[DynamicMessage]]
                .asScala
                .foreach { mapEntry =>
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
                  val maybeKey = k.getJavaType match {
                    // According to Protobuf docs, "the key_type can be any integral or string type"
                    // https://developers.google.com/protocol-buffers/docs/proto3#maps
                    case STRING => Some(mapEntry.getField(k).asInstanceOf[String])
                    case INT | LONG | BOOLEAN => Some(mapEntry.getField(k).toString)
                    case other =>
                      logger.warn(
                        safe"Cannot process the key ${Safe(other.toString)}. Protobuf can only accept keys of type String, Boolean, Integer. This map key will be ignored.",
                      )
                      None
                  }
                  maybeKey.map(key =>
                    localMapBuilder.add(key, fieldToValue(v.getJavaType, mapEntry.getField(v), folder)),
                  )
                }

              localMapBuilder.finish()

            } else {
              val vecBuilder = folder.vectorBuilder()
              message
                .getField(field)
                .asInstanceOf[java.util.List[AnyRef]]
                .asScala
                .map(f => fieldToValue(field.getJavaType, f, folder))
                .foreach(vecBuilder.add)
              vecBuilder.finish()

            }
          } else {
            fieldToValue(field.getJavaType, message.getField(field), folder)

          }
        }
        mapBuilder.add(field.getName, b)
      }
      mapBuilder.finish()
    }
  }

  implicit val avroDataFoldable: DataFoldableFrom[GenericRecord] = new DataFoldableFrom[GenericRecord] {
    private def foldMapLike[B](kv: Iterable[(String, Any)], folder: DataFolderTo[B]): B = {
      val mapBuilder = folder.mapBuilder()
      kv.foreach { case (k, v) => mapBuilder.add(k, foldField(v, folder)) }
      mapBuilder.finish()
    }

    // All of the underlying types for avro were taken from here: https://stackoverflow.com/questions/34070028/get-a-typed-value-from-an-avro-genericrecord/34234039#34234039
    private def foldField[B](field: Any, folder: DataFolderTo[B]): B = field match {
      case b: java.lang.Boolean if b => folder.trueValue
      case b: java.lang.Boolean if !b => folder.falseValue
      case i: java.lang.Integer => folder.integer(i.longValue)
      case i: java.lang.Long => folder.integer(i)
      case f: java.lang.Float => folder.floating(f.doubleValue)
      case d: java.lang.Double => folder.floating(d)
      case bytes: java.nio.ByteBuffer => folder.bytes(bytes.array)
      case str: CharSequence => folder.string(str.toString)
      case record: GenericRecord =>
        foldMapLike(
          record.getSchema.getFields.asScala.collect {
            case k if record.hasField(k.name) => (k.name, record.get(k.name))
          },
          folder,
        )
      case map: java.util.Map[_, _] => foldMapLike(map.asScala.map { case (k, v) => (k.toString, v) }, folder)
      case symbol: GenericEnumSymbol[_] => folder.string(symbol.toString)
      case array: GenericArray[_] =>
        val vector = folder.vectorBuilder()
        array.forEach(elem => vector.add(foldField(elem, folder)))
        vector.finish()
      case fixed: GenericFixed => folder.bytes(fixed.bytes)
      case n if n == null => folder.nullValue
      case other =>
        throw new IllegalArgumentException(
          s"Got an unexpected value: ${other} of type: ${other.getClass.getName} from avro. This shouldn't happen...",
        )
    }

    override def fold[B](record: GenericRecord, folder: DataFolderTo[B]): B = foldField(record, folder)
  }
}

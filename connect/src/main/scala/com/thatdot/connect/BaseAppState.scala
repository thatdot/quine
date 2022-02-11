package com.thatdot.connect

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

import endpoints4s.{Codec, Invalid, Valid, Validated}

import com.thatdot.quine.graph.{BaseGraph, MemberIdx}

/** Applications running over top of Quine should define an application state that extends this.
  * Then, individual settings can be stored here (for easy persistence, reset, etc). Under the hood,
  * this will take advantage of the persistor APIs for meta-data
  *
  * @param graph reference to the underlying graph
  */
abstract class BaseAppState(graph: BaseGraph) extends endpoints4s.ujson.JsonSchemas {

  implicit val ec: ExecutionContextExecutor = graph.system.dispatcher // TODO: use other dispatcher?

  /** Store a key-value pair that is relevant only for one particular app instance (i.e. "local")
    *
    * @note the value is serialized as the UTF-8 bytes of its JSON representation
    * @param key name of the setting
    * @param value setting value
    */
  final protected def storeLocalMetaData[A](key: String, localMemberId: MemberIdx, value: A)(implicit
    schema: JsonSchema[A]
  ): Future[Unit] =
    graph.persistor.setLocalMetaData(key, localMemberId, Some(encodeMetaData(value)))

  /** Store a key-value pair that is relevant for the cluster as a whole.
    *
    * @note the value is serialized as the UTF-8 bytes of its JSON representation
    * @param key name of the setting
    * @param value setting value
    */
  final protected def storeClusterMetaData[A](key: String, value: A)(implicit schema: JsonSchema[A]): Future[Unit] =
    graph.persistor.setMetaData(key, Some(encodeMetaData(value)))

  /** Serialize a value intended to be stored as metadata for the cluster or the local app.
    *
    * @param value the value to be serialized as the UTF-8 bytes of its JSON representation
    * @param schema an endpoints4s ujson schema derived to provide the string codec
    * @tparam A The type of the value to be encoded
    * @return The encoded value as a byte array
    */
  final protected def encodeMetaData[A](value: A)(implicit schema: JsonSchema[A]): Array[Byte] =
    Codec.sequentially(BaseAppState.utf8Codec)(schema.stringCodec).encode(value)

  /** Retrieve a value associated with a key which was stored for the local app
    *
    * @note the value is serialized as the UTF-8 bytes of its JSON representation
    * @param key name of the setting
    * @return the value, if found
    */
  final protected def getLocalMetaData[A](key: String, localMemberId: MemberIdx)(implicit
    schema: JsonSchema[A]
  ): Future[Option[A]] =
    graph.persistor.getLocalMetaData(key, localMemberId).map {
      _.flatMap { jsonBytes =>
        Some(validateMetaData(decodeMetaData(jsonBytes)(schema))) // throws to fail the future
      }
    }

  /** Retrieve a value associated with a key which was stored for the cluster as a whole
    *
    * @note the value is serialized as the UTF-8 bytes of its JSON representation
    * @param key name of the setting
    * @return the value, if found
    */
  final protected def getClusterMetaData[A](key: String)(implicit schema: JsonSchema[A]): Future[Option[A]] =
    graph.persistor.getMetaData(key).map {
      _.flatMap { jsonBytes =>
        Some(validateMetaData(decodeMetaData(jsonBytes)(schema))) // throws to fail the future
      }
    }

  /** Deserialize a value intended to be stored as metadata for the cluster or the local app.
    *
    * @param value the value serialized value as the UTF-8 bytes of its JSON representation to be deserialized
    * @param schema an endpoints4s ujson schema derived to provide the string codec
    * @tparam A The type of the value to be encoded
    * @return The encoded value as a byte array
    */
  final protected def decodeMetaData[A](jsonBytes: Array[Byte])(implicit schema: JsonSchema[A]): Validated[A] =
    Codec.sequentially(BaseAppState.utf8Codec)(schema.stringCodec).decode(jsonBytes)

  /** A convenience method for unwrapping the decoded (validated) deserialized value. Throws an exception if invalid.
    *
    * @param decoded the deserialized metadata value; likely returned from `decodeMetaData`
    * @tparam A the type for which the bytes are being deserialized
    * @throws if the bytes fail to be deserialized as the intended type
    * @return the deserialized type
    */
  @throws[MetaDataDeserializationException]
  final def validateMetaData[A](decoded: Validated[A]): A = decoded match {
    case Valid(value) => value
    case Invalid(errs) => throw new MetaDataDeserializationException(errs.mkString("\n"))
  }

  /** Retrieve a value associated with a key stored for this local app, but write and return in a default value
    * if the key is not already defined for the local app
    *
    * @note the value is serialized as the UTF-8 bytes of its JSON representation
    * @param key name of the setting
    * @param defaultValue default setting value
    * @return the (possibly updated) value
    */
  final protected def getOrDefaultLocalMetaData[A: JsonSchema](
    key: String,
    localMemberId: MemberIdx,
    defaultValue: => A
  ): Future[A] =
    getLocalMetaData[A](key, localMemberId).flatMap {
      case Some(value) => Future.successful(value)
      case None =>
        val defaulted = defaultValue
        storeLocalMetaData(key, localMemberId, defaulted).map(_ => defaulted)
    }

  /** Retrieve a value associated with a key stored for the cluster as a whole, but write and return in a default value
    * if the key is not already defined for the cluster
    *
    * @note the value is serialized as the UTF-8 bytes of its JSON representation
    * @param key name of the setting
    * @param defaultValue default setting value
    * @return the (possibly updated) value
    */
  final protected def getOrDefaultClusterMetaData[A: JsonSchema](key: String, defaultValue: => A): Future[A] =
    getClusterMetaData[A](key).flatMap {
      case Some(value) => Future.successful(value)
      case None =>
        val defaulted = defaultValue
        storeClusterMetaData(key, defaulted).map(_ => defaulted)
    }
}

object BaseAppState {

  /** Codec for UTF-8 strings */
  private val utf8Codec = new Codec[Array[Byte], String] {
    private[this] val decoder = UTF_8.newDecoder()

    def encode(str: String): Array[Byte] = str.getBytes(UTF_8)
    def decode(bytes: Array[Byte]): Validated[String] = Validated.fromTry(Try {
      val result = decoder.decode(ByteBuffer.wrap(bytes))
      result.toString
    })
  }
}

class MetaDataDeserializationException(msg: String) extends RuntimeException(msg)

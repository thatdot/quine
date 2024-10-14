package com.thatdot.quine.app

import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

import endpoints4s.{Invalid, Valid, Validated}
import io.circe.{Encoder, jawn}

import com.thatdot.quine.app.QuineApp.{V2IngestStreamsKey, makeNamespaceMetaDataKey}
import com.thatdot.quine.app.serialization.EncoderDecoder
import com.thatdot.quine.graph.{BaseGraph, MemberIdx, NamespaceId}

/** Applications running over top of Quine should define an application state that extends this.
  * Then, individual settings can be stored here (for easy persistence, reset, etc). Under the hood,
  * this will take advantage of the persistor APIs for meta-data
  *
  * @param graph reference to the underlying graph
  */
abstract class BaseApp(graph: BaseGraph) {

  /** Store a key-value pair that is relevant only for one particular app instance (i.e. "local")
    *
    * @note the value is serialized as the UTF-8 bytes of its JSON representation
    * @param key name of the setting
    * @param value setting value
    */
  final protected def storeLocalMetaData[A: EncoderDecoder](
    key: String,
    localMemberId: MemberIdx,
    value: A,
  ): Future[Unit] =
    graph.namespacePersistor.setLocalMetaData(key, localMemberId, Some(encodeMetaData(value)))

  /** Store a key-value pair that is relevant for the entire graph
    *
    * @note the value is serialized as the UTF-8 bytes of its JSON representation
    * @param key name of the setting
    * @param value setting value
    */
  final protected def storeGlobalMetaData[A: EncoderDecoder](key: String, value: A): Future[Unit] =
    graph.namespacePersistor.setMetaData(key, Some(encodeMetaData(value)))

  final protected def deleteGlobalMetaData(key: String): Future[Unit] =
    graph.namespacePersistor.setMetaData(key, None)

  /** Serialize a value intended to be stored as metadata
    *
    * @param value the value to be serialized as the UTF-8 bytes of its JSON representation
    * @param schema an endpoints4s ujson schema derived to provide the string codec
    * @tparam A The type of the value to be encoded
    * @return The encoded value as a byte array
    */
  final protected def encodeMetaData[A](value: A)(implicit encoderDecoder: EncoderDecoder[A]): Array[Byte] =
    encoderDecoder.encoder(value).noSpaces.getBytes(UTF_8)
  final protected def encodeMetaData[A](value: A, encoder: Encoder[A]): Array[Byte] =
    encoder(value).noSpaces.getBytes(UTF_8)

  /** Retrieve a value associated with a key which was stored for the local app
    *
    * @note the value is serialized as the UTF-8 bytes of its JSON representation
    * @param key name of the setting
    * @return the value, if found
    */
  final protected def getLocalMetaData[A](key: String, localMemberId: MemberIdx)(implicit
    encoderDecoder: EncoderDecoder[A],
  ): Future[Option[A]] =
    graph.namespacePersistor
      .getLocalMetaData(key, localMemberId)
      .map {
        _.flatMap { jsonBytes =>
          Some(validateMetaData(decodeMetaData(jsonBytes)(encoderDecoder))) // throws to fail the future
        }
      }(graph.system.dispatcher)

  /** Retrieve a value associated with a key which was stored for the entire graph
    *
    * @note the value is serialized as the UTF-8 bytes of its JSON representation
    * @param key name of the setting
    * @return the value, if found
    */
  final protected def getGlobalMetaData[A](key: String)(implicit encoderDecoder: EncoderDecoder[A]): Future[Option[A]] =
    graph.namespacePersistor
      .getMetaData(key)
      .map {
        _.flatMap { jsonBytes =>
          Some(validateMetaData(decodeMetaData(jsonBytes)(encoderDecoder))) // throws to fail the future
        }
      }(graph.system.dispatcher)

  /** Deserialize a value intended to be stored as metadata
    *
    * @param value the value serialized value as the UTF-8 bytes of its JSON representation to be deserialized
    * @param schema an endpoints4s ujson schema derived to provide the string codec
    * @tparam A The type of the value to be encoded
    * @return The encoded value as a byte array
    */
  final protected def decodeMetaData[A](jsonBytes: Array[Byte])(implicit
    encoderDecoder: EncoderDecoder[A],
  ): Validated[A] =
    Validated.fromEither(jawn.decodeByteArray(jsonBytes)(encoderDecoder.decoder).left.map(err => Seq(err.toString)))
  //Codec.sequentially(BaseApp.utf8Codec)(schema.stringCodec).decode(jsonBytes)

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
  final protected def getOrDefaultLocalMetaData[A: EncoderDecoder](
    key: String,
    localMemberId: MemberIdx,
    defaultValue: => A,
  ): Future[A] =
    getLocalMetaData[A](key, localMemberId).flatMap {
      case Some(value) => Future.successful(value)
      case None =>
        val defaulted = defaultValue
        storeLocalMetaData(key, localMemberId, defaulted).map(_ => defaulted)(graph.system.dispatcher)
    }(graph.system.dispatcher)

  protected def saveV2IngestsToPersistor[IngestWithStatusType: EncoderDecoder](
    namespace: NamespaceId,
    thisMemberIdx: Int,
    ingests: Map[String, IngestWithStatusType],
    key: String = V2IngestStreamsKey,
  ): Future[Unit] =
    storeLocalMetaData[Map[String, IngestWithStatusType]](
      makeNamespaceMetaDataKey(namespace, key),
      thisMemberIdx,
      ingests,
    )(EncoderDecoder.ofMap)

  protected def loadV2IngestsFromPersistor[IngestWithStatusType: EncoderDecoder](
    thisMemberIdx: Int,
    key: String = V2IngestStreamsKey,
  )(implicit ex: ExecutionContext): Future[Map[NamespaceId, Map[String, IngestWithStatusType]]] = Future
    .sequence(
      getNamespaces.map(ns =>
        getOrDefaultLocalMetaData[Map[String, IngestWithStatusType]](
          makeNamespaceMetaDataKey(ns, key),
          thisMemberIdx,
          Map.empty[String, IngestWithStatusType],
        )(EncoderDecoder.ofMap).map(v => ns -> v),
      ),
    )
    .map(_.toMap)

  /** Retrieve a value associated with a key stored for this local app, but write and return in a default value
    * if the key is not already defined for the local app. Upon encountering an unrecognized value, will attempt
    * to decode as type B and convert to type A. Used for backwards-compatible migrations.
    *
    * @note NOT threadsafe. Should be used in synchronized contexts
    * @note the value is serialized as the UTF-8 bytes of its JSON representation
    * @param key          name of the setting
    * @param defaultValue default setting value
    * @param recovery     a function converting a value from the fallback schema to the desired schema
    * @return the (possibly updated) value
    */
  final protected def getOrDefaultLocalMetaDataWithFallback[A: EncoderDecoder, B: EncoderDecoder](
    key: String,
    localMemberId: MemberIdx,
    defaultValue: => A,
    recovery: B => A,
  ): Future[A] =
    getLocalMetaData[A](key, localMemberId)
      .flatMap {
        case Some(value) => Future.successful(value)
        case None =>
          val defaulted = defaultValue
          storeLocalMetaData(key, localMemberId, defaulted).map(_ => defaulted)(graph.system.dispatcher)
      }(graph.system.dispatcher)
      .recoverWith { case _: MetaDataDeserializationException =>
        getLocalMetaData[B](key, localMemberId).flatMap {
          case Some(value) => Future.successful(recovery(value))
          case None =>
            val defaulted = defaultValue
            storeLocalMetaData(key, localMemberId, defaulted).map(_ => defaulted)(graph.system.dispatcher)
        }(graph.system.dispatcher)
      }(graph.nodeDispatcherEC)

  (graph.system.dispatcher)

  /** Retrieve a value associated with a key stored for the entire graph as a
    * whole, but write and return in a default value if the key is not already
    * defined.
    *
    * @note the value is serialized as the UTF-8 bytes of its JSON representation
    * @param key name of the setting
    * @param defaultValue default setting value
    * @return the (possibly updated) value
    */
  final protected def getOrDefaultGlobalMetaData[A: EncoderDecoder](key: String, defaultValue: => A): Future[A] =
    getGlobalMetaData[A](key).flatMap {
      case Some(value) => Future.successful(value)
      case None =>
        val defaulted = defaultValue
        storeGlobalMetaData(key, defaulted).map(_ => defaulted)(graph.system.dispatcher)
    }(graph.system.dispatcher)

  /** Instantiate a new namespace to store nodes separately.
    * @param namespace the name of the new namespace to be created
    * @param shouldWriteToPersistor True for all individual runtime operations. False during startup while rehydrating.
    * @return Future status according to persistence. Boolean indicates whether a chance was made.
    */
  def createNamespace(namespace: NamespaceId, shouldWriteToPersistor: Boolean = true): Future[Boolean] =
    Future.failed(new UnsupportedOperationException(s"Namespace management is not supported."))

  /** Delete an existing namespace and all the data in it.
    * @param namespace the name of the new namespace to be deleted
    * @return Future status according to persistence. Boolean indicates whether a chance was made.
    */
  def deleteNamespace(namespace: NamespaceId): Future[Boolean] =
    Future.failed(new UnsupportedOperationException(s"Namespace management is not supported."))

  /** Reads the local cache of available namespaces. */
  def getNamespaces: collection.Set[NamespaceId] = graph.getNamespaces

  def onlyIfNamespaceExists[A](namespace: NamespaceId)(f: => Future[A]): Future[A] =
    if (getNamespaces.contains(namespace)) f
    else Future.failed(NamespaceNotFoundException(namespace))

  def noneIfNoNamespace[A](namespace: NamespaceId)(f: => Option[A]): Option[A] =
    if (getNamespaces.contains(namespace)) f
    else None

  def failIfNoNamespace[A](namespace: NamespaceId)(f: => Try[A]): Try[A] =
    if (getNamespaces.contains(namespace)) f
    else Failure(NamespaceNotFoundException(namespace))
}

class MetaDataDeserializationException(msg: String) extends RuntimeException(msg)

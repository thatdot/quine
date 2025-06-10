package com.thatdot.quine.serialization

import java.net.URL

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters._
import scala.util.Using

import com.amazonaws.services.schemaregistry.utils.apicurio.DynamicSchema
import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}
import com.google.protobuf.Descriptors.{Descriptor, DescriptorValidationException}
import com.google.protobuf.InvalidProtocolBufferException

import com.thatdot.quine.serialization.ProtobufSchemaError.{
  AmbiguousMessageType,
  InvalidProtobufSchema,
  NoSuchMessageType,
  UnreachableProtobufSchema,
}
import com.thatdot.quine.util.ComputeAndBlockingExecutionContext

/** Provides common utilities for its inheritors to parse protobuf descriptors.
  *
  * @see [[com.thatdot.quine.app.model.ingest.serialization.ProtobufParser]]
  * @see [[QuineValueToProtobuf]]
  */
trait ProtobufSchemaCache {

  /** Get a parser for the given schema and type name. Failures (exceptions of type [[ProtobufSchemaError]])
    * are returned as a Future.failed
    */
  def getSchema(schemaUrl: URL): Future[DynamicSchema]

  /** Get a parser for the given schema and type name. If the schema is unreachable or invalid, the
    * schema will not be cached. Similarly, when [[flushOnFail]] is true, if the type name is not found,
    * or is ambiguous, the schema will not be cached.
    */
  def getMessageDescriptor(schemaUrl: URL, typeName: String, flushOnFail: Boolean): Future[Descriptor]
}
object ProtobufSchemaCache {

  @deprecated("For test/docs use only -- all Futures are blocking. Use LoadingCache instead.", "forever and always")
  object Blocking extends ProtobufSchemaCache {
    def getSchema(schemaUrl: URL): Future[DynamicSchema] =
      resolveSchema(schemaUrl)(ExecutionContext.parasitic)
    def getMessageDescriptor(schemaUrl: URL, typeName: String, flushOnFail: Boolean): Future[Descriptor] =
      getSchema(schemaUrl)
        .map(resolveMessageType(typeName))(ExecutionContext.parasitic)
        .flatMap {
          case Right(descriptor) => Future.successful(descriptor)
          case Left(error) => Future.failed(error)
        }(ExecutionContext.parasitic)
  }

  class AsyncLoading(val ecs: ComputeAndBlockingExecutionContext) extends ProtobufSchemaCache {
    private val parsedDescriptorCache: AsyncLoadingCache[URL, DynamicSchema] =
      Scaffeine()
        .maximumSize(5)
        .buildAsyncFuture { schemaUrl =>
          // NB if this Future fails (with a [[ProtobufSchemaError]]), the cache will not store the schema.
          // This allows the user to retry the schema resolution after updating their environment
          resolveSchema(schemaUrl)(ecs.blockingDispatcherEC)
        }

    /** Invalidate the schema for the given URI. This will cause the next call to [[parsedDescriptorCache.get]]
      * to re-parse the schema. This may be desirable when, for example, a message type lookup fails, even if the
      * schema lookup succeeds (so that the user can update their schema file to include the missing type).
      */
    def flush(uri: URL): Unit =
      parsedDescriptorCache.put(uri, Future.successful(null))

    def getSchema(schemaUrl: URL): Future[DynamicSchema] =
      parsedDescriptorCache.get(schemaUrl)

    def getMessageDescriptor(schemaUrl: URL, typeName: String, flushOnFail: Boolean): Future[Descriptor] =
      getSchema(schemaUrl)
        .map(resolveMessageType(typeName))(ecs.nodeDispatcherEC)
        .flatMap {
          case Right(descriptor) => Future.successful(descriptor)
          case Left(error) =>
            if (flushOnFail) flush(schemaUrl)
            Future.failed(error)
        }(ecs.nodeDispatcherEC)
  }

  private[this] def resolveSchema(uri: URL)(blockingEc: ExecutionContext): Future[DynamicSchema] =
    Future(blocking {
      Using.resource(uri.openStream())(DynamicSchema.parseFrom)
    })(blockingEc).recoverWith {
      case e: DescriptorValidationException => Future.failed(new InvalidProtobufSchema(uri, e))
      case e: InvalidProtocolBufferException =>
        // InvalidProtocolBufferException <: java.io.IOException, so this case needs to come before the IOException one
        Future.failed(new InvalidProtobufSchema(uri, e))
      case e: java.io.IOException => Future.failed(new UnreachableProtobufSchema(uri, e))
    }(ExecutionContext.parasitic)

  /** Given a schema, resolve the message type by name, coercing errors to [[ProtobufSchemaError]]s.
    * This function is cheap to call (i.e., it doesn't need caching), as it is just a lookup in an
    * already-populated map.
    */
  private[this] def resolveMessageType(messageType: String)(
    schema: DynamicSchema,
  ): Either[ProtobufSchemaMessageTypeException, Descriptor] =
    Option(schema.getMessageDescriptor(messageType)).toRight {
      // failure cases: either the type doesn't exist, or it's ambiguous
      val resolvedMessageTypes = schema.getMessageTypes.asScala.toSet
      val messageFoundByFullName = resolvedMessageTypes.contains(messageType)
      val messagesFoundByShortName = resolvedMessageTypes.filter(_.split(raw"\.").contains(messageType))

      if (!messageFoundByFullName && messagesFoundByShortName.isEmpty)
        new NoSuchMessageType(messageType, resolvedMessageTypes)
      else {
        // We failed to resolve, but the type exists... this must be because the type is ambiguous as-provided
        new AmbiguousMessageType(messageType, messagesFoundByShortName)
      }
    }
}

package com.thatdot.quine.app.serialization

import java.net.URL

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Using

import com.amazonaws.services.schemaregistry.utils.apicurio.DynamicSchema
import com.google.protobuf.Descriptors.{Descriptor, DescriptorValidationException}
import com.google.protobuf.InvalidProtocolBufferException

import com.thatdot.quine.app.serialization.ProtobufSchemaError._
import com.thatdot.quine.util.{ComputeAndBlockingExecutionContext, FromSingleExecutionContext}

/** Provides common utilities for its inheritors to parse protobuf descriptors.
  *
  * @see [[com.thatdot.quine.app.ingest.serialization.ProtobufParser]]
  * @see [[QuineValueToProtobuf]]
  * @throws UnreachableProtobufSchema If opening the schema file fails
  * @throws InvalidProtobufSchema     If the schema file is invalid
  * @throws NoSuchMessageType         If the schema file does not contain the specified type
  * @throws AmbiguousMessageType      If the schema file contains multiple message descriptors that
  *                                   all match the provided name
  */
abstract class ProtobufSchema(schemaUrl: URL, typeName: String) {
  // TODO QU-1868: All of this IO and validating that the type name exists in the file should
  //   be done in the async-aware cache, not blocking the thread at class construction time
  //   and throwing exceptions.
  final protected val messageType: Descriptor =
    Await.result(
      ProtobufSchema.descriptorForType(schemaUrl, typeName)(new FromSingleExecutionContext(ExecutionContext.parasitic)),
      Duration.Inf
    )
}

object ProtobufSchema {

  private[this] def resolveSchema(uri: URL)(blockingEc: ExecutionContext): Future[DynamicSchema] =
    Future(Using.resource(uri.openStream())(DynamicSchema.parseFrom))(blockingEc).recoverWith {
      case e: DescriptorValidationException => Future.failed(new InvalidProtobufSchema(uri, e))
      case e: InvalidProtocolBufferException =>
        // InvalidProtocolBufferException <: java.io.IOException, so this case needs to come before the IOException one
        Future.failed(new InvalidProtobufSchema(uri, e))
      case e: java.io.IOException => Future.failed(new UnreachableProtobufSchema(uri, e))
    }(ExecutionContext.parasitic)

  private[this] def resolveMessageType(
    schema: DynamicSchema,
    messageType: String
  ): Either[ProtobufSchemaError, Descriptor] =
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

  private def descriptorForType(schemaUri: URL, typeName: String)(
    ecs: ComputeAndBlockingExecutionContext
  ): Future[Descriptor] =
    resolveSchema(schemaUri)(ecs.blockingDispatcherEC)
      .flatMap(schema =>
        resolveMessageType(schema, typeName) match {
          case Left(err) => Future.failed(err)
          case Right(value) => Future.successful(value)
        }
      )(ecs.nodeDispatcherEC)

}

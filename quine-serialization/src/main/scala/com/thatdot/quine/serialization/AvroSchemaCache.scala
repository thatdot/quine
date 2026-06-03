package com.thatdot.quine.serialization

import java.net.URL

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.Using

import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}
import org.apache.avro.{Schema, SchemaParseException}

import com.thatdot.quine.serialization.AvroSchemaError.{InvalidAvroSchema, UnreachableAvroSchema}
import com.thatdot.quine.util.ComputeAndBlockingExecutionContext

/** Provides common utilities for its inheritors to parse avro objects.
  */
trait AvroSchemaCache {
  def getSchema(schemaUrl: URL): Future[Schema]

}
object AvroSchemaCache {
  class AsyncLoading(val ecs: ComputeAndBlockingExecutionContext) extends AvroSchemaCache {
    private val avroSchemaCache: AsyncLoadingCache[URL, Schema] =
      Scaffeine()
        .maximumSize(5)
        .buildAsyncFuture { schemaUrl =>
          // NB if this Future fails (with an error), the cache will not store the schema.
          // This allows the user to retry the schema resolution after updating their environment
          resolveSchema(schemaUrl)(ecs.blockingDispatcherEC)
        }

    /** Invalidate the schema for the given URI. This will cause the next call to [[avroSchemaCache.get]]
      * to re-parse the schema. This may be desirable when, for example, a message type lookup fails, even if the
      * schema lookup succeeds (so that the user can update their schema file to include the missing type).
      */
    def flush(uri: URL): Unit =
      avroSchemaCache.put(uri, Future.successful(null))

    def getSchema(schemaUrl: URL): Future[Schema] =
      avroSchemaCache.get(schemaUrl)

    private[this] def resolveSchema(uri: URL)(blockingEc: ExecutionContext): Future[Schema] =
      Future(blocking {
        // Schema.Parser is stateful (accumulates named types) and not thread-safe,
        // so a fresh parser per call is required.
        val parser = new Schema.Parser()
        Using.resource(uri.openStream())(parser.parse)
      })(blockingEc).recoverWith {
        case e: SchemaParseException => Future.failed(new InvalidAvroSchema(uri, e))
        case e: java.io.IOException => Future.failed(new UnreachableAvroSchema(uri, e))
      }(blockingEc)
  }
}

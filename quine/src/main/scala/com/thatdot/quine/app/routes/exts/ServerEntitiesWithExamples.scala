package com.thatdot.quine.app.routes.exts

import java.io.InputStream

import scala.concurrent.Future

import akka.http.scaladsl.model.{HttpEntity, MediaTypes, StatusCodes}
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.unmarshalling._
import akka.stream.Materializer
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.scaladsl.{Sink, StreamConverters}

import endpoints4s.akkahttp.server.JsonEntitiesFromSchemas
import endpoints4s.{Invalid, Valid, Validated}

import com.thatdot.quine.app.yaml
import com.thatdot.quine.routes.exts.NoopEntitiesWithExamples
import com.thatdot.quine.util.QuineDispatchers

trait ServerEntitiesWithExamples
    extends NoopEntitiesWithExamples
    with JsonEntitiesFromSchemas
    with endpoints4s.akkahttp.server.EndpointsWithCustomErrors {

  def csvRequest: RequestEntity[List[List[String]]] = {
    implicit val um: FromEntityUnmarshaller[List[List[String]]] =
      Unmarshaller
        .withMaterializer { _ => implicit mat => (entity: HttpEntity) =>
          val charset = Unmarshaller.bestUnmarshallingCharsetFor(entity).nioCharset
          entity.dataBytes
            .via(CsvParsing.lineScanner())
            .map(_.view.map(_.decodeString(charset)).toList)
            .named("csv-unmarshaller")
            .runWith(Sink.collection[List[String], List[List[String]]])
        }
        .forContentTypes(MediaTypes.`text/csv`)

    Directives
      .entity[List[List[String]]](implicitly)
  }

  private def requestEntityAsInputStream(entity: HttpEntity)(implicit materializer: Materializer): InputStream =
    entity.dataBytes.runWith(StreamConverters.asInputStream())(materializer)

  def yamlRequest[A](implicit schema: JsonSchema[A]): RequestEntity[A] = {
    val yamlUnmarshaller: FromRequestUnmarshaller[Validated[A]] =
      Unmarshaller
        .withMaterializer(_ =>
          implicit mat =>
            request =>
              Future {
                // While the conversion from Akka Stream Source to a java.io.InputStream
                // does not block, the subsequent use of the InputStream (yaml.parseToJson)
                // does involve blocking "io", hence that is done on a blocking thread.
                // "the users of the materialized value, InputStream, [...] will block" - akka/akka#30831
                val requestInputStream = requestEntityAsInputStream(request.entity)
                schema.decoder.decode(
                  yaml.parseToJson(requestInputStream)
                )
              }(new QuineDispatchers(mat.system).blockingDispatcherEC)
        )

    Directives.entity[Validated[A]](yamlUnmarshaller).flatMap {
      case Valid(a) => Directives.provide(a)
      case inv: Invalid => handleClientErrors(inv)
    }
  }

  val ServiceUnavailable = StatusCodes.ServiceUnavailable
}

package com.thatdot.quine.app.routes.exts

import akka.http.scaladsl.model.{HttpEntity, MediaTypes, StatusCodes}
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.unmarshalling._
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.scaladsl.Sink

import com.thatdot.quine.routes.exts.NoopEntitiesWithExamples

trait ServerEntitiesWithExamples
    extends NoopEntitiesWithExamples
    with endpoints4s.akkahttp.server.EndpointsWithCustomErrors {

  def csvRequest: RequestEntity[List[List[String]]] = {
    implicit val um: FromEntityUnmarshaller[List[List[String]]] =
      Unmarshaller
        .withMaterializer { _ => implicit mat => (entity: HttpEntity) =>
          val charset = Unmarshaller.bestUnmarshallingCharsetFor(entity).nioCharset
          entity.dataBytes
            .via(CsvParsing.lineScanner())
            .map(_.view.map(_.decodeString(charset)).toList)
            .runWith(Sink.collection[List[String], List[List[String]]])
        }
        .forContentTypes(MediaTypes.`text/csv`)

    Directives
      .entity[List[List[String]]](implicitly)
  }

  def ServiceUnavailable = StatusCodes.ServiceUnavailable
}

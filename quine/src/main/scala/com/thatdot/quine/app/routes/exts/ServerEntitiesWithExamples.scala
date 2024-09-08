package com.thatdot.quine.app.routes.exts

import java.io.InputStream

import scala.concurrent.Future

import org.apache.pekko.http.scaladsl.model.{ContentTypeRange, HttpEntity, MediaTypes, StatusCodes}
import org.apache.pekko.http.scaladsl.server.{Directive1, Directives}
import org.apache.pekko.http.scaladsl.unmarshalling._
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.connectors.csv.scaladsl.CsvParsing
import org.apache.pekko.stream.scaladsl.{Sink, StreamConverters}

import io.circe.yaml.v12.Parser
import org.snakeyaml.engine.v2.api.YamlUnicodeReader

import com.thatdot.quine.app.routes.exts.circe.JsonEntitiesFromSchemas
import com.thatdot.quine.app.routes.{MediaTypes => QuineMediaTypes}
import com.thatdot.quine.routes.exts.NoopEntitiesWithExamples
import com.thatdot.quine.util.QuineDispatchers
trait ServerEntitiesWithExamples
    extends NoopEntitiesWithExamples
    with JsonEntitiesFromSchemas
    with endpoints4s.pekkohttp.server.EndpointsWithCustomErrors {

  /** Helper function for turning an HTTP request body => A function into a Pekko Unmarshaller / Directive */
  protected def unmarshallerFor[A](
    contentType: ContentTypeRange,
  )(
    f: (Materializer, HttpEntity) => Future[A],
  ): Directive1[A] = Directives.entity[A](
    Unmarshaller.messageUnmarshallerFromEntityUnmarshaller(
      Unmarshaller.withMaterializer[HttpEntity, A](_ => mat => entity => f(mat, entity)).forContentTypes(contentType),
    ),
  )
  lazy val csvRequest: RequestEntity[List[List[String]]] =
    unmarshallerFor(MediaTypes.`text/csv`) { (mat, entity) =>
      val charset = Unmarshaller.bestUnmarshallingCharsetFor(entity).nioCharset
      entity.dataBytes
        .via(CsvParsing.lineScanner())
        .map(_.view.map(_.decodeString(charset)).toList)
        .named("csv-unmarshaller")
        .runWith(Sink.collection[List[String], List[List[String]]])(mat)
    }

  private def requestEntityAsInputStream(entity: HttpEntity)(materializer: Materializer): InputStream =
    entity.dataBytes.runWith(StreamConverters.asInputStream())(materializer)

  def yamlRequest[A](implicit schema: JsonSchema[A]): RequestEntity[A] =
    unmarshallerFor(QuineMediaTypes.`application/yaml`)((mat, entity) =>
      Future {
        // While the conversion from Pekko Stream Source to a java.io.InputStream
        // does not block, the subsequent use of the InputStream (yaml.parseToJson)
        // does involve blocking "io", hence that is done on a blocking thread.
        // "the users of the materialized value, InputStream, [...] will block" - akka/akka#30831
        val requestInputStream = requestEntityAsInputStream(entity)(mat)
        Parser.default.decodeAccumulating(new YamlUnicodeReader(requestInputStream))(schema.decoder)
      }(new QuineDispatchers(mat.system).blockingDispatcherEC),
    ).flatMap(circeDecodeResultToEndpointsDirective)

  val ServiceUnavailable = StatusCodes.ServiceUnavailable
}

package com.thatdot.quine.app.routes.exts.circe

import org.apache.pekko.http.scaladsl.server.{Directive1, Directives}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshaller

import cats.data.ValidatedNel
import com.github.pjfanning.pekkohttpcirce.ErrorAccumulatingCirceSupport
import endpoints4s.pekkohttp.server.EndpointsWithCustomErrors
import endpoints4s.{algebra, circe}

trait JsonEntitiesFromSchemas
    extends circe.JsonSchemas
    with algebra.JsonEntitiesFromSchemas
    with EndpointsWithCustomErrors {
  def jsonRequest[A](implicit schema: JsonSchema[A]): RequestEntity[A] = Directives
    .entity(
      Unmarshaller.messageUnmarshallerFromEntityUnmarshaller(
        ErrorAccumulatingCirceSupport.safeUnmarshaller[A](schema.decoder),
      ),
    )
    .flatMap(circeDecodeResultToEndpointsDirective)

  // Helper for every time we have to do this. The return type from circe decoding, Validated[NonEmptyList[Error], A], is very like
  // endpoints4s's Validated[Seq[String], A] - we just need to translate from the former to the latter for HTTP response.
  // Basically .toList.map(_.toString) to turn the NonEmptyList[Error] into a Seq[String]
  protected def circeDecodeResultToEndpointsDirective[A](
    jsonDecodingResult: ValidatedNel[io.circe.Error, A],
  ): Directive1[A] = jsonDecodingResult.fold(
    errors => handleClientErrors(endpoints4s.Invalid(errors.toList.map(io.circe.Error.showError.show))),
    a => Directives.provide(a),
  )
  def jsonResponse[A](implicit schema: JsonSchema[A]): ResponseEntity[A] =
    ErrorAccumulatingCirceSupport.marshaller[A](schema.encoder)

}

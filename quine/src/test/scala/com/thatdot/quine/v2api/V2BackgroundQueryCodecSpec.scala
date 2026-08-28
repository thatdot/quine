package com.thatdot.quine.v2api

import cats.data.NonEmptyList
import io.circe.parser.decode
import io.circe.syntax._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import sttp.tapir.Schema
import sttp.tapir.SchemaType.{SCoproduct, SProduct}

import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.endpoints.Action
import com.thatdot.quine.app.v2api.endpoints.V2BackgroundQueryEndpointEntities.BackgroundQueryDef

/** The run-now body (`BackgroundQueryDef`) and the flattened job action (`Action.BackgroundQuery`)
  * share the same query fields and must behave identically: `parameters` is optional both at decode
  * time (defaults to `{}`) and in the OpenAPI schema (not listed as required), `query` and
  * `destinations` are required, and unknown fields are rejected.
  */
class V2BackgroundQueryCodecSpec extends AnyFunSpec with Matchers {

  private val drop: NonEmptyList[QuineDestinationSteps] = NonEmptyList.one(QuineDestinationSteps.Drop)
  private val destJson: String = drop.asJson.noSpaces

  /** The field names tapir would emit in the schema's `required` list: non-optional fields with no
    * default. Flattens a single-variant coproduct (the discriminated `Action`) into its product.
    */
  private def requiredFields(schema: Schema[_]): Set[String] = schema.schemaType match {
    case p: SProduct[_] =>
      p.fields.collect { case f if !f.schema.isOptional && f.schema.default.isEmpty => f.name.name }.toSet
    case c: SCoproduct[_] => c.subtypes.flatMap(requiredFields).toSet
    case _ => Set.empty
  }

  describe("BackgroundQueryDef") {
    it("treats parameters as optional at decode time, defaulting to {}") {
      decode[BackgroundQueryDef](s"""{"query":"RETURN 1","destinations":$destJson}""") shouldBe
      Right(BackgroundQueryDef(query = "RETURN 1", destinations = drop))
    }
    it("requires destinations") {
      decode[BackgroundQueryDef]("""{"query":"RETURN 1"}""").isLeft shouldBe true
    }
    it("round-trips") {
      val def0 = BackgroundQueryDef(query = "RETURN 1", destinations = drop, name = Some("n"))
      def0.asJson.as[BackgroundQueryDef] shouldBe Right(def0)
    }
    it("rejects unknown fields (strict decoding)") {
      decode[BackgroundQueryDef](
        s"""{"query":"RETURN 1","destinations":$destJson,"nonsense":true}""",
      ).isLeft shouldBe true
    }
    // `destinations` is a NonEmptyList, derived as an array schema; tapir never lists collection fields
    // in OpenAPI `required` (an empty array would satisfy the type there) — non-emptiness is instead
    // enforced at decode time (see "requires destinations"). So only `query` is schema-required here.
    it("marks query required but parameters not required in the schema") {
      val required = requiredFields(implicitly[Schema[BackgroundQueryDef]])
      required should contain("query")
      required should not contain "parameters"
    }
  }

  describe("Action.BackgroundQuery") {
    it("treats parameters as optional at decode time, defaulting to {}") {
      decode[Action](s"""{"type":"BackgroundQuery","query":"RETURN 1","destinations":$destJson}""") shouldBe
      Right(Action.BackgroundQuery(query = "RETURN 1", destinations = drop))
    }
    it("marks query required but parameters not required in the schema") {
      val required = requiredFields(implicitly[Schema[Action]])
      required should contain("query")
      required should not contain "parameters"
    }
  }
}

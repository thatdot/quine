package com.thatdot.api.v2

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.customise

/** A miniature of the shared API model, with a field, a type and a variant that belong to some products only. */
object ProductSpecificSpecModel {
  @customise(ProductSpecific.enterpriseOnly)
  final case class ClusterSettings(members: Int)

  sealed trait Login
  final case class PlainLogin(user: String) extends Login
  @customise(ProductSpecific.only(ProductVersion.Enterprise, ProductVersion.Novelty))
  final case class AssertionLogin(certFile: String) extends Login

  final case class Destination(
    login: Option[Login],
    @customise(ProductSpecific.enterpriseOnly) memberIdx: Option[Int],
    labels: Map[String, Login],
  )

  final case class Definition(destinations: List[Destination], cluster: Option[ClusterSettings])

  implicit lazy val clusterSettingsSchema: Schema[ClusterSettings] = Schema.derived
  implicit lazy val loginSchema: Schema[Login] = Schema.derived
  implicit lazy val destinationSchema: Schema[Destination] = Schema.derived
  implicit lazy val definitionSchema: Schema[Definition] = Schema.derived

  /** Marks the spec would lose or that other products could not satisfy. */
  final case class Misplaced(
    @customise(ProductSpecific.enterpriseOnly) settings: Option[Destination],
    @customise(ProductSpecific.enterpriseOnly) required: Int,
    @customise(ProductSpecific.enterpriseOnly) fine: Option[Int],
    alsoFine: Option[ClusterSettings],
  )
  implicit lazy val misplacedSchema: Schema[Misplaced] = Schema.derived
}

class ProductSpecificSpec extends AnyFunSuite with Matchers with OptionValues {
  import ProductSpecificSpecModel._

  test("reports, at once and by path, every field, type and variant the product does not support") {
    val definition = Definition(
      destinations = List(
        Destination(login = Some(AssertionLogin("/certs")), memberIdx = None, labels = Map.empty),
        Destination(
          login = Some(PlainLogin("alice")),
          memberIdx = Some(2),
          labels = Map("plain" -> PlainLogin("bob"), "a b" -> AssertionLogin("/other")),
        ),
      ),
      cluster = Some(ClusterSettings(3)),
    )

    def fieldsRefusedIn(product: ProductVersion): List[String] =
      ProductSpecific.validate(definition, product).fold(List.empty[String])(_.violations.map(_.field))

    fieldsRefusedIn(ProductVersion.Oss) should contain theSameElementsAs List(
      "destinations[0].login",
      "destinations[1].memberIdx",
      "destinations[1].labels.`a b`",
      "cluster",
    )
    fieldsRefusedIn(ProductVersion.Novelty) should contain theSameElementsAs List(
      "destinations[1].memberIdx",
      "cluster",
    )
    fieldsRefusedIn(ProductVersion.Enterprise) shouldBe empty

    val refusal = ProductSpecific.validate(definition, ProductVersion.Oss).value
    refusal.toBadRequest.details shouldBe List(ErrorDetail.BadRequest(refusal.violations))
    refusal.violations.map(_.description).distinct shouldBe List("Not supported in Quine.")
    ProductSpecific.validate(definition, ProductVersion.Enterprise) shouldBe None
  }

  test("finds marks the published spec would lose, or that other products could not leave out") {
    ProductSpecific.misplacedMarks(misplacedSchema) should have size 2
    ProductSpecific.misplacedMarks(misplacedSchema).mkString("\n") should (include("settings") and include("required"))
    ProductSpecific.misplacedMarks(definitionSchema) shouldBe empty
  }
}

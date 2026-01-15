package com.thatdot.quine.webapp2.router

import cats.syntax.functor._
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._

sealed abstract class QuineOssPage(val title: String)
object QuineOssPage {
  case object ExplorerUi extends QuineOssPage("Exploration UI")
  case object DocsV1 extends QuineOssPage("Interactive Documentation V1")
  case object DocsV2 extends QuineOssPage("Interactive Documentation V2")
  case object Metrics extends QuineOssPage("Metrics")

  implicit val ExplorerUiPageDecoder: Decoder[ExplorerUi.type] = deriveDecoder
  implicit val ExplorerUiPageEncoder: Encoder[ExplorerUi.type] = deriveEncoder

  implicit val DocsV1PageDecoder: Decoder[DocsV1.type] = deriveDecoder
  implicit val DocsV1PageEncoder: Encoder[DocsV1.type] = deriveEncoder

  implicit val DocV2PageDecoder: Decoder[DocsV2.type] = deriveDecoder
  implicit val DocsV2PageEncoder: Encoder[DocsV2.type] = deriveEncoder

  implicit val metricsPageDecoder: Decoder[Metrics.type] = deriveDecoder
  implicit val metricsPageEncoder: Encoder[Metrics.type] = deriveEncoder

  implicit val PageDecoder: Decoder[QuineOssPage] =
    List[Decoder[QuineOssPage]](Decoder[ExplorerUi.type].widen, Decoder[DocsV1.type].widen).reduceLeft(_ or _)

  implicit val PageEncoder: Encoder[QuineOssPage] = Encoder.instance {
    case ExplorerUi => ExplorerUi.asJson
    case DocsV1 => DocsV1.asJson
    case DocsV2 => DocsV2.asJson
    case Metrics => Metrics.asJson
  }
}

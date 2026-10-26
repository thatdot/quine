package com.thatdot.quine.webapp.components.streams

import scala.concurrent.ExecutionContext

import com.raquo.laminar.api.L._
import io.circe.Json

/** The assists a form adds to every Novelty destination it renders, each beneath the field it fills
  * in: [[NoveltyDiscoverInstances]] under `endpoints`, and [[NoveltySampleGenerate]] under
  * `transformationFunction`.
  */
object NoveltyAssists {

  /** Name of the destination's schema in the API spec. Only Quine Enterprise's spec has it. */
  private val NoveltySchema = "Novelty"

  /** @param live      where to sample live results from, when the form's standing query exists
    * @param formState the form's state, which the assists write into
    * @param serverUrl the Quine API's base URL (empty ⇒ same origin)
    */
  def apply(live: Option[NoveltySampleGenerate.LiveSample], formState: Var[Json], serverUrl: String)(implicit
    ec: ExecutionContext,
  ): FieldAddons =
    FieldAddons {
      case (NoveltySchema, "endpoints", destinationPath) =>
        Some(NoveltyDiscoverInstances(destinationPath, formState, serverUrl))
      case (NoveltySchema, "transformationFunction", destinationPath) =>
        Some(NoveltySampleGenerate(live, destinationPath, formState))
      case _ => None
    }
}

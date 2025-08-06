package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.{ExecutionContext, Future}

import io.circe.generic.extras.auto._
import sttp.model.StatusCode
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{emptyOutputAs, statusCode}

import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions.ApiUiStyling.{SampleQuery, UiNodeAppearance, UiNodeQuickQuery}
import com.thatdot.quine.app.v2api.definitions.ErrorResponseHelpers.serverError
import com.thatdot.quine.app.v2api.definitions.{SuccessEnvelope, V2QuineEndpointDefinitions}

trait V2UiStylingEndpoints extends V2QuineEndpointDefinitions with StringOps {

  private def uiStylingEndpoint =
    rawEndpoint("query-ui")
      .tag("UI Styling")
      .description(
        """Operations for customizing parts of the Query UI. These options are generally useful for tailoring the UI
          |to a particular domain or data model (e.g. to customize the icon, color, size, context-menu queries, etc.
          |for nodes based on their contents).""".asOneLine,
      )
      .errorOut(serverError())

  lazy val uiEndpoints: List[ServerEndpoint[Any, Future]] = List(
    queryUiSampleQueries,
    updateQueryUiSampleQueries,
    queryUiAppearance,
    updateQueryUiAppearance,
    queryUiQuickQueries,
    updateQueryUiQuickQueries,
  )

  private val queryUiSampleQueries = uiStylingEndpoint
    .name("List Sample Queries")
    .description("Queries provided here will be available via a drop-down menu from the Quine UI search bar.")
    .get
    .in("sample-queries")
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[Vector[SampleQuery]]].example(SuccessEnvelope.Ok(SampleQuery.defaults)))
    .serverLogic[Future] { _ =>
      recoverServerError(
        appMethods.getSamplesQueries(ExecutionContext.parasitic),
      )(SuccessEnvelope.Ok(_))
    }

  private val updateQueryUiSampleQueries = uiStylingEndpoint
    .name("Replace Sample Queries")
    .description(
      "Queries provided here will be available via a drop-down menu from the Quine UI search bar.\n\n" +
      "Queries applied here will replace any currently existing sample queries.",
    )
    .put
    .in("sample-queries")
    .in(jsonOrYamlBody[Vector[SampleQuery]](Some(SampleQuery.defaults)))
    .out(statusCode(StatusCode.NoContent))
    .out(emptyOutputAs(SuccessEnvelope.NoContent))
    .serverLogic[Future] { newSampleQueries =>
      recoverServerError(appMethods.setSampleQueries(newSampleQueries))(_ => SuccessEnvelope.NoContent)
    }

  private val queryUiAppearance = uiStylingEndpoint
    .name("List Node Appearances ")
    .description(
      """When rendering a node in the UI, a node's style is decided by picking the first style in this list whose
        |`predicate` matches the node.""".asOneLine,
    )
    .get
    .in("node-appearances")
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[Vector[UiNodeAppearance]]].example {
      SuccessEnvelope.Ok(UiNodeAppearance.defaults)
    })
    .serverLogic[Future] { _ =>
      recoverServerError(appMethods.getNodeAppearances(ExecutionContext.parasitic))(SuccessEnvelope.Ok(_))
    }

  private val updateQueryUiAppearance = uiStylingEndpoint
    .name("Replace Node Appearances")
    .description("For a list of icon names, refer to [this page](https://ionicons.com/v2/cheatsheet.html)")
    .put
    .in("node-appearances")
    .in(jsonOrYamlBody[Vector[UiNodeAppearance]](Some(UiNodeAppearance.defaults)))
    .out(statusCode(StatusCode.NoContent))
    .out(emptyOutputAs(SuccessEnvelope.NoContent))
    .serverLogic[Future] { q =>
      recoverServerError(appMethods.setNodeAppearances(q))(_ => SuccessEnvelope.NoContent)
    }

  private val queryUiQuickQueries = uiStylingEndpoint
    .name("List Quick Queries")
    .description(
      """Quick queries are queries that appear when right-clicking a node in the UI.
        |Nodes will only display quick queries that satisfy any provided predicates.""".asOneLine,
    )
    .get
    .in("quick-queries")
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[Vector[UiNodeQuickQuery]]].example {
      SuccessEnvelope.Ok(UiNodeQuickQuery.defaults)
    })
    .serverLogic[Future] { _ =>
      recoverServerError(appMethods.getQuickQueries(ExecutionContext.parasitic))(SuccessEnvelope.Ok(_))
    }

  private val updateQueryUiQuickQueries = uiStylingEndpoint
    .name("Replace Quick Queries")
    .description(
      """Quick queries are queries that appear when right-clicking a node in the UI.
        |Queries applied here will replace any currently existing quick queries.""".asOneLine,
    )
    .put
    .in("quick-queries")
    .in(jsonOrYamlBody[Vector[UiNodeQuickQuery]](Some(UiNodeQuickQuery.defaults)))
    .out(statusCode(StatusCode.NoContent))
    .out(emptyOutputAs(SuccessEnvelope.NoContent))
    .serverLogic[Future] { q =>
      recoverServerError(appMethods.setQuickQueries(q))(_ => SuccessEnvelope.NoContent)
    }
}

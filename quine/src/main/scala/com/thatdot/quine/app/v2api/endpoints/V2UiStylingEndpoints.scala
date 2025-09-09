package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.{ExecutionContext, Future}

import io.circe.generic.extras.auto._
import sttp.model.StatusCode
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Endpoint, emptyOutputAs, statusCode}

import com.thatdot.api.v2.ErrorResponseHelpers.serverError
import com.thatdot.api.v2.{ErrorResponse, SuccessEnvelope}
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions.ApiUiStyling.{SampleQuery, UiNodeAppearance, UiNodeQuickQuery}
import com.thatdot.quine.app.v2api.definitions.V2QuineEndpointDefinitions

trait V2UiStylingEndpoints extends V2QuineEndpointDefinitions with StringOps {

  private val uiStylingBase: EndpointBase = rawEndpoint("query-ui")
    .tag("UI Styling")
    .description(
      """Operations for customizing parts of the Query UI. These options are generally useful for tailoring the UI
          |to a particular domain or data model (e.g. to customize the icon, color, size, context-menu queries, etc.
          |for nodes based on their contents).""".asOneLine,
    )
    .errorOut(serverError())

  protected[endpoints] val queryUiSampleQueries
    : Endpoint[Unit, Unit, ErrorResponse.ServerError, SuccessEnvelope.Ok[Vector[SampleQuery]], Any] = uiStylingBase
    .name("List Sample Queries")
    .description("Queries provided here will be available via a drop-down menu from the Quine UI search bar.")
    .get
    .in("sample-queries")
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[Vector[SampleQuery]]].example(SuccessEnvelope.Ok(SampleQuery.defaults)))

  protected[endpoints] val queryUiSampleQueriesLogic
    : Unit => Future[Either[ErrorResponse.ServerError, SuccessEnvelope.Ok[Vector[SampleQuery]]]] = _ =>
    recoverServerError(appMethods.getSamplesQueries(ExecutionContext.parasitic))(SuccessEnvelope.Ok(_))

  private val queryUiSampleQueriesServerEndpoint
    : Full[Unit, Unit, Unit, ErrorResponse.ServerError, SuccessEnvelope.Ok[Vector[SampleQuery]], Any, Future] =
    queryUiSampleQueries.serverLogic[Future](queryUiSampleQueriesLogic)

  protected[endpoints] val updateQueryUiSampleQueries
    : Endpoint[Unit, Vector[SampleQuery], ErrorResponse.ServerError, SuccessEnvelope.NoContent.type, Any] =
    uiStylingBase
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

  protected[endpoints] val updateQueryUiSampleQueriesLogic
    : Vector[SampleQuery] => Future[Either[ErrorResponse.ServerError, SuccessEnvelope.NoContent.type]] =
    newSampleQueries =>
      recoverServerError(appMethods.setSampleQueries(newSampleQueries))(_ => SuccessEnvelope.NoContent)

  private val updateQueryUiSampleQueriesServerEndpoint
    : Full[Unit, Unit, Vector[SampleQuery], ErrorResponse.ServerError, SuccessEnvelope.NoContent.type, Any, Future] =
    updateQueryUiSampleQueries.serverLogic[Future](updateQueryUiSampleQueriesLogic)

  protected[endpoints] val queryUiAppearance
    : Endpoint[Unit, Unit, ErrorResponse.ServerError, SuccessEnvelope.Ok[Vector[UiNodeAppearance]], Any] =
    uiStylingBase
      .name("List Node Appearances ")
      .description(
        """When rendering a node in the UI, a node's style is decided by picking the first style in this list whose
        |`predicate` matches the node.""".asOneLine,
      )
      .get
      .in("node-appearances")
      .out(statusCode(StatusCode.Ok))
      .out(
        jsonBody[SuccessEnvelope.Ok[Vector[UiNodeAppearance]]].example(SuccessEnvelope.Ok(UiNodeAppearance.defaults)),
      )

  protected[endpoints] val queryUiAppearanceLogic
    : Unit => Future[Either[ErrorResponse.ServerError, SuccessEnvelope.Ok[Vector[UiNodeAppearance]]]] = _ =>
    recoverServerError(appMethods.getNodeAppearances(ExecutionContext.parasitic))(SuccessEnvelope.Ok(_))

  private val queryUiAppearanceServerEndpoint
    : Full[Unit, Unit, Unit, ErrorResponse.ServerError, SuccessEnvelope.Ok[Vector[UiNodeAppearance]], Any, Future] =
    queryUiAppearance.serverLogic[Future](queryUiAppearanceLogic)

  protected[endpoints] val updateQueryUiAppearance
    : Endpoint[Unit, Vector[UiNodeAppearance], ErrorResponse.ServerError, SuccessEnvelope.NoContent.type, Any] =
    uiStylingBase
      .name("Replace Node Appearances")
      .description("For a list of icon names, refer to [this page](https://ionicons.com/v2/cheatsheet.html)")
      .put
      .in("node-appearances")
      .in(jsonOrYamlBody[Vector[UiNodeAppearance]](Some(UiNodeAppearance.defaults)))
      .out(statusCode(StatusCode.NoContent))
      .out(emptyOutputAs(SuccessEnvelope.NoContent))

  protected[endpoints] val updateQueryUiAppearanceLogic
    : Vector[UiNodeAppearance] => Future[Either[ErrorResponse.ServerError, SuccessEnvelope.NoContent.type]] = q =>
    recoverServerError(appMethods.setNodeAppearances(q))(_ => SuccessEnvelope.NoContent)

  private val updateQueryUiAppearanceServerEndpoint: Full[
    Unit,
    Unit,
    Vector[UiNodeAppearance],
    ErrorResponse.ServerError,
    SuccessEnvelope.NoContent.type,
    Any,
    Future,
  ] = updateQueryUiAppearance.serverLogic[Future](updateQueryUiAppearanceLogic)

  protected[endpoints] val queryUiQuickQueries: Endpoint[
    Unit,
    Unit,
    ErrorResponse.ServerError,
    SuccessEnvelope.Ok[Vector[UiNodeQuickQuery]],
    Any,
  ] = uiStylingBase
    .name("List Quick Queries")
    .description(
      """Quick queries are queries that appear when right-clicking a node in the UI.
        |Nodes will only display quick queries that satisfy any provided predicates.""".asOneLine,
    )
    .get
    .in("quick-queries")
    .out(statusCode(StatusCode.Ok))
    .out(
      jsonBody[SuccessEnvelope.Ok[Vector[UiNodeQuickQuery]]].example(SuccessEnvelope.Ok(UiNodeQuickQuery.defaults)),
    )

  protected[endpoints] val queryUiQuickQueriesLogic
    : Unit => Future[Either[ErrorResponse.ServerError, SuccessEnvelope.Ok[Vector[UiNodeQuickQuery]]]] = _ =>
    recoverServerError(appMethods.getQuickQueries(ExecutionContext.parasitic))(SuccessEnvelope.Ok(_))

  private val queryUiQuickQueriesServerEndpoint
    : Full[Unit, Unit, Unit, ErrorResponse.ServerError, SuccessEnvelope.Ok[Vector[UiNodeQuickQuery]], Any, Future] =
    queryUiQuickQueries.serverLogic[Future](queryUiQuickQueriesLogic)

  protected[endpoints] val updateQueryUiQuickQueries
    : Endpoint[Unit, Vector[UiNodeQuickQuery], ErrorResponse.ServerError, SuccessEnvelope.NoContent.type, Any] =
    uiStylingBase
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

  protected[endpoints] val updateQueryUiQuickQueriesLogic
    : Vector[UiNodeQuickQuery] => Future[Either[ErrorResponse.ServerError, SuccessEnvelope.NoContent.type]] = q =>
    recoverServerError(appMethods.setQuickQueries(q))(_ => SuccessEnvelope.NoContent)

  private val updateQueryUiQuickQueriesServerEndpoint: Full[
    Unit,
    Unit,
    Vector[UiNodeQuickQuery],
    ErrorResponse.ServerError,
    SuccessEnvelope.NoContent.type,
    Any,
    Future,
  ] = updateQueryUiQuickQueries.serverLogic[Future](updateQueryUiQuickQueriesLogic)

  lazy val uiEndpoints: List[ServerEndpoint[Any, Future]] = List(
    queryUiSampleQueriesServerEndpoint,
    updateQueryUiSampleQueriesServerEndpoint,
    queryUiAppearanceServerEndpoint,
    updateQueryUiAppearanceServerEndpoint,
    queryUiQuickQueriesServerEndpoint,
    updateQueryUiQuickQueriesServerEndpoint,
  )

}

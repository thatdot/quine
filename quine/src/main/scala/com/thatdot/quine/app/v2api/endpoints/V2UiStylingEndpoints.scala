package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.{ExecutionContext, Future}

import sttp.model.StatusCode
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Endpoint, emptyOutputAs, statusCode}

import com.thatdot.api.v2.ErrorResponse
import com.thatdot.api.v2.ErrorResponseHelpers.serverError
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions.ApiUiStyling.{SampleQuery, UiNodeAppearance, UiNodeQuickQuery}
import com.thatdot.quine.app.v2api.definitions.V2QuineEndpointDefinitions

trait V2UiStylingEndpoints extends V2QuineEndpointDefinitions with StringOps {

  private val uiStylingBase: EndpointBase = rawEndpoint("queryUi")
    .tag("UI Styling")
    .description(
      """Operations for customizing parts of the Query UI. These options are generally useful for tailoring the UI
          |to a particular domain or data model (e.g. to customize the icon, color, size, context-menu queries, etc.
          |for nodes based on their contents).""".asOneLine,
    )
    .errorOut(serverError())

  // The three GET list endpoints in this trait — sample queries, node appearances, quick
  // queries — return bare arrays instead of the AIP-158 `Page[T]` envelope used elsewhere
  // for user-resource collections. These are admin-curated UI configuration blobs bounded
  // by what fits in the Query UI dropdowns/menus; pagination would never apply, and an
  // always-empty `nextPageToken` would mislead clients into thinking the response might be
  // truncated. The corresponding PUT endpoints replace the entire array atomically, which
  // also makes them poor candidates for paged consumption.

  protected[endpoints] val queryUiSampleQueries
    : Endpoint[Unit, Unit, ErrorResponse.ServerError, Vector[SampleQuery], Any] = uiStylingBase
    .name("list-sample-queries")
    .summary("List Sample Queries")
    .description("Queries provided here will be available via a drop-down menu from the Quine UI search bar.")
    .get
    .in("sampleQueries")
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[Vector[SampleQuery]].example(SampleQuery.defaults))

  protected[endpoints] val queryUiSampleQueriesLogic
    : Unit => Future[Either[ErrorResponse.ServerError, Vector[SampleQuery]]] = _ =>
    recoverServerError(appMethods.getSamplesQueries(ExecutionContext.parasitic))(identity)

  private val queryUiSampleQueriesServerEndpoint
    : Full[Unit, Unit, Unit, ErrorResponse.ServerError, Vector[SampleQuery], Any, Future] =
    queryUiSampleQueries.serverLogic[Future](queryUiSampleQueriesLogic)

  protected[endpoints] val updateQueryUiSampleQueries
    : Endpoint[Unit, Vector[SampleQuery], ErrorResponse.ServerError, Unit, Any] =
    uiStylingBase
      .name("replace-sample-queries")
      .summary("Replace Sample Queries")
      .description(
        "Queries provided here will be available via a drop-down menu from the Quine UI search bar.\n\n" +
        "Queries applied here will replace any currently existing sample queries.",
      )
      .put
      .in("sampleQueries")
      .in(jsonOrYamlBody[Vector[SampleQuery]](Some(SampleQuery.defaults)))
      .out(statusCode(StatusCode.NoContent))
      .out(emptyOutputAs(()))

  protected[endpoints] val updateQueryUiSampleQueriesLogic
    : Vector[SampleQuery] => Future[Either[ErrorResponse.ServerError, Unit]] =
    newSampleQueries => recoverServerError(appMethods.setSampleQueries(newSampleQueries))(_ => ())

  private val updateQueryUiSampleQueriesServerEndpoint
    : Full[Unit, Unit, Vector[SampleQuery], ErrorResponse.ServerError, Unit, Any, Future] =
    updateQueryUiSampleQueries.serverLogic[Future](updateQueryUiSampleQueriesLogic)

  protected[endpoints] val queryUiAppearance
    : Endpoint[Unit, Unit, ErrorResponse.ServerError, Vector[UiNodeAppearance], Any] =
    uiStylingBase
      .name("list-node-appearances")
      .summary("List Node Appearances")
      .description(
        """When rendering a node in the UI, a node's style is decided by picking the first style in this list whose
        |`predicate` matches the node.""".asOneLine,
      )
      .get
      .in("nodeAppearances")
      .out(statusCode(StatusCode.Ok))
      .out(
        jsonBody[Vector[UiNodeAppearance]].example(UiNodeAppearance.defaults),
      )

  protected[endpoints] val queryUiAppearanceLogic
    : Unit => Future[Either[ErrorResponse.ServerError, Vector[UiNodeAppearance]]] = _ =>
    recoverServerError(appMethods.getNodeAppearances(ExecutionContext.parasitic))(identity)

  private val queryUiAppearanceServerEndpoint
    : Full[Unit, Unit, Unit, ErrorResponse.ServerError, Vector[UiNodeAppearance], Any, Future] =
    queryUiAppearance.serverLogic[Future](queryUiAppearanceLogic)

  protected[endpoints] val updateQueryUiAppearance
    : Endpoint[Unit, Vector[UiNodeAppearance], ErrorResponse.ServerError, Unit, Any] =
    uiStylingBase
      .name("replace-node-appearances")
      .summary("Replace Node Appearances")
      .description("For a list of icon names, refer to [this page](https://ionicons.com/v2/cheatsheet.html)")
      .put
      .in("nodeAppearances")
      .in(jsonOrYamlBody[Vector[UiNodeAppearance]](Some(UiNodeAppearance.defaults)))
      .out(statusCode(StatusCode.NoContent))
      .out(emptyOutputAs(()))

  protected[endpoints] val updateQueryUiAppearanceLogic
    : Vector[UiNodeAppearance] => Future[Either[ErrorResponse.ServerError, Unit]] = q =>
    recoverServerError(appMethods.setNodeAppearances(q))(_ => ())

  private val updateQueryUiAppearanceServerEndpoint: Full[
    Unit,
    Unit,
    Vector[UiNodeAppearance],
    ErrorResponse.ServerError,
    Unit,
    Any,
    Future,
  ] = updateQueryUiAppearance.serverLogic[Future](updateQueryUiAppearanceLogic)

  protected[endpoints] val queryUiQuickQueries: Endpoint[
    Unit,
    Unit,
    ErrorResponse.ServerError,
    Vector[UiNodeQuickQuery],
    Any,
  ] = uiStylingBase
    .name("list-quick-queries")
    .summary("List Quick Queries")
    .description(
      """Quick queries are queries that appear when right-clicking a node in the UI.
        |Nodes will only display quick queries that satisfy any provided predicates.""".asOneLine,
    )
    .get
    .in("quickQueries")
    .out(statusCode(StatusCode.Ok))
    .out(
      jsonBody[Vector[UiNodeQuickQuery]].example(UiNodeQuickQuery.defaults),
    )

  protected[endpoints] val queryUiQuickQueriesLogic
    : Unit => Future[Either[ErrorResponse.ServerError, Vector[UiNodeQuickQuery]]] = _ =>
    recoverServerError(appMethods.getQuickQueries(ExecutionContext.parasitic))(identity)

  private val queryUiQuickQueriesServerEndpoint
    : Full[Unit, Unit, Unit, ErrorResponse.ServerError, Vector[UiNodeQuickQuery], Any, Future] =
    queryUiQuickQueries.serverLogic[Future](queryUiQuickQueriesLogic)

  protected[endpoints] val updateQueryUiQuickQueries
    : Endpoint[Unit, Vector[UiNodeQuickQuery], ErrorResponse.ServerError, Unit, Any] =
    uiStylingBase
      .name("replace-quick-queries")
      .summary("Replace Quick Queries")
      .description(
        """Quick queries are queries that appear when right-clicking a node in the UI.
        |Queries applied here will replace any currently existing quick queries.""".asOneLine,
      )
      .put
      .in("quickQueries")
      .in(jsonOrYamlBody[Vector[UiNodeQuickQuery]](Some(UiNodeQuickQuery.defaults)))
      .out(statusCode(StatusCode.NoContent))
      .out(emptyOutputAs(()))

  protected[endpoints] val updateQueryUiQuickQueriesLogic
    : Vector[UiNodeQuickQuery] => Future[Either[ErrorResponse.ServerError, Unit]] = q =>
    recoverServerError(appMethods.setQuickQueries(q))(_ => ())

  private val updateQueryUiQuickQueriesServerEndpoint: Full[
    Unit,
    Unit,
    Vector[UiNodeQuickQuery],
    ErrorResponse.ServerError,
    Unit,
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

package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.webapp.util.PollingStream

/** A "list of named resources with a create form" card, abstracted over the
  * specific resource type.
  *
  * Wraps the polling, refresh-on-action, and Table/Form view toggle that the
  * Ingest and Standing Query panels share verbatim. Callers supply only the
  * resource-specific labels and the Form / Table renderers.
  */
object StreamCollectionPanel {

  private val PollIntervalMs = 5.seconds.toMillis.toInt

  sealed trait PanelView
  object PanelView {
    case object Table extends PanelView
    case object CreateForm extends PanelView
  }

  /** Render the panel.
    *
    * @param listFn       function that fetches the resource list (called on each poll tick
    *                     and on manual refresh)
    * @param renderCreateForm takes (onComplete, onCancel). `onComplete` flips the
    *                     view back to the table AND triggers a manual refresh;
    *                     `onCancel` only flips the view.
    * @param renderTable  takes the entries signal and a refresh thunk that the
    *                     table can call after a row-level mutation.
    */
  def apply(
    title: String,
    newLabel: String,
    emptyMessage: String,
    emptyCta: String,
    listFn: () => Future[Either[String, Json]],
    renderCreateForm: (() => Unit, () => Unit) => HtmlElement,
    renderTable: (Signal[List[(String, Json)]], () => Unit) => HtmlElement,
  ): HtmlElement = {
    val viewVar = Var[PanelView](PanelView.Table)
    val dataVar = Var[Option[Either[String, Json]]](None)
    val refreshBus = new EventBus[Unit]

    val pollStream: EventStream[Either[String, Json]] = {
      val fetchStream = PollingStream(PollIntervalMs)(listFn())
      val manualStream = refreshBus.events.flatMapSwitch { _ =>
        EventStream.fromFuture(listFn())
      }
      fetchStream.mergeWith(manualStream)
    }

    def refresh(): Unit = refreshBus.emit(())

    val entriesSignal: Signal[List[(String, Json)]] = dataVar.signal.map {
      case Some(Right(json)) => normalizeList(json)
      case _ => Nil
    }

    // `.distinct` is essential: only emits on state-category transitions, so the
    // table element (and any inline form DOM inside it) stays mounted across
    // per-poll content updates.
    val tableState: Signal[String] = dataVar.signal.map {
      case None => "loading"
      case Some(Left(_)) => "error"
      case Some(Right(json)) if normalizeList(json).isEmpty => "empty"
      case Some(Right(_)) => "ok"
    }.distinct

    div(
      cls := "card",
      pollStream --> { result => dataVar.set(Some(result)) },
      div(
        cls := "card-header d-flex justify-content-between align-items-center",
        h5(cls := "mb-0", title),
        child <-- viewVar.signal.map {
          case PanelView.Table =>
            button(
              cls := "btn btn-primary btn-sm",
              i(cls := "cil-plus me-1"),
              newLabel,
              onClick --> { _ => viewVar.set(PanelView.CreateForm) },
            )
          case PanelView.CreateForm =>
            button(
              cls := "btn btn-secondary btn-sm",
              "Back to List",
              onClick --> { _ => viewVar.set(PanelView.Table) },
            )
        },
      ),
      // Card body — only depends on viewVar so that polling updates to dataVar
      // do not rebuild the form subtree (which would discard its input state).
      div(
        cls := "card-body",
        child <-- viewVar.signal.map {
          case PanelView.CreateForm =>
            renderCreateForm(
              () => { viewVar.set(PanelView.Table); refresh() },
              () => viewVar.set(PanelView.Table),
            )
          case PanelView.Table =>
            div(
              child <-- tableState.map {
                case "loading" =>
                  div(
                    cls := "text-center py-4",
                    div(cls := "spinner-border text-primary", role := "status"),
                  )
                case "error" =>
                  div(
                    cls := "alert alert-danger mb-0",
                    child.text <-- dataVar.signal.map {
                      case Some(Left(err)) => err
                      case _ => ""
                    },
                  )
                case "empty" =>
                  div(
                    cls := "text-center text-body-secondary py-4",
                    p(emptyMessage),
                    button(
                      cls := "btn btn-primary",
                      i(cls := "cil-plus me-1"),
                      emptyCta,
                      onClick --> { _ => viewVar.set(PanelView.CreateForm) },
                    ),
                  )
                case _ =>
                  renderTable(entriesSignal, () => refresh())
              },
            )
        },
      ),
    )
  }

  /** Parse a V2 list response: `[{"name": "n1", ...}, ...]`. Items missing a
    * `name` field are tagged "unknown" so the row still appears (matches the
    * original per-panel behaviour).
    */
  private def normalizeList(json: Json): List[(String, Json)] =
    json.asArray
      .getOrElse(Vector.empty)
      .toList
      .map { item =>
        val name = item.hcursor.get[String]("name").getOrElse("unknown")
        name -> item
      }
}

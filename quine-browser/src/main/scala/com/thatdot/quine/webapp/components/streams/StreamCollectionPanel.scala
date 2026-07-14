package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.util.Pot

/** A "list of named resources with a create form" card, abstracted over the
  * specific resource type.
  *
  * Wraps the shared-feed consumption, refresh-on-action, and Table/Form view toggle that
  * the Ingest and Standing Query panels share verbatim. Callers supply only the
  * resource-specific labels, the keyed entries feed, and the Form / Table renderers.
  */
object StreamCollectionPanel {

  sealed trait PanelView
  object PanelView {
    case object Table extends PanelView
    case object CreateForm extends PanelView
  }

  /** Render the panel.
    *
    * @param canCreate    whether the user may create this resource; when false the
    *                     "New" button and empty-state CTA are not rendered
    * @param entries      keyed rows from the shared DataService feed. The first tuple element
    *                     is a stable row identity, not a display name (clustered ingests fold
    *                     `memberIdx` into it so same-named rows don't collide).
    * @param onRefresh    asks the service to refetch the list immediately (dispatched after
    *                     mutations instead of waiting out the poll interval)
    * @param renderCreateForm takes (onComplete, onCancel). `onComplete` flips the
    *                     view back to the table AND triggers a refresh;
    *                     `onCancel` only flips the view.
    * @param renderTable  takes the entries signal and a refresh thunk that the
    *                     table can call after a row-level mutation.
    */
  def apply[A](
    title: String,
    newLabel: String,
    emptyMessage: String,
    emptyCta: String,
    canCreate: Boolean,
    entries: Signal[Pot[List[(String, A)]]],
    onRefresh: () => Unit,
    renderCreateForm: (() => Unit, () => Unit) => HtmlElement,
    renderTable: (Signal[List[(String, A)]], () => Unit) => HtmlElement,
  ): HtmlElement = {
    val viewVar = Var[PanelView](PanelView.Table)
    // Draft copy of the feed. A refresh restarts the underlying poll, which re-emits
    // `Pending`; holding the last-shown rows through that as `PendingStale` keeps the
    // table mounted instead of flashing its spinner after every mutation.
    val dataVar = Var[Pot[List[(String, A)]]](Pot.Pending)

    val entriesSignal: Signal[List[(String, A)]] = dataVar.signal.map(_.toOption.getOrElse(Nil))

    // `.distinct` is essential: only emits on state-category transitions, so the
    // table element (and any inline form DOM inside it) stays mounted across
    // per-poll content updates.
    val tableState: Signal[String] = dataVar.signal.map {
      case Pot.Empty | Pot.Pending => "loading"
      case Pot.Failed(_) => "error"
      case withValue => if (withValue.toOption.exists(_.isEmpty)) "empty" else "ok"
    }.distinct

    div(
      cls := "card",
      entries --> { pot =>
        dataVar.update { prev =>
          (pot, prev.toOption) match {
            case (Pot.Pending, Some(rows)) => Pot.PendingStale(rows)
            case _ => pot
          }
        }
      },
      div(
        cls := "card-header d-flex justify-content-between align-items-center",
        h5(cls := "mb-0", title),
        child <-- viewVar.signal.map {
          case PanelView.Table if canCreate =>
            button(
              cls := "btn btn-primary btn-sm",
              i(cls := "cil-plus me-1"),
              newLabel,
              onClick --> { _ => viewVar.set(PanelView.CreateForm) },
            )
          case PanelView.Table =>
            emptyNode
          case PanelView.CreateForm =>
            button(
              cls := "btn btn-secondary btn-sm",
              "Back to List",
              onClick --> { _ => viewVar.set(PanelView.Table) },
            )
        },
      ),
      // Card body — only depends on viewVar so that feed updates to dataVar
      // do not rebuild the form subtree (which would discard its input state).
      div(
        cls := "card-body",
        child <-- viewVar.signal.map {
          case PanelView.CreateForm =>
            renderCreateForm(
              () => { viewVar.set(PanelView.Table); onRefresh() },
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
                      case Pot.Failed(err) => err
                      case _ => ""
                    },
                  )
                case "empty" =>
                  div(
                    cls := "text-center text-body-secondary py-4",
                    p(emptyMessage),
                    if (canCreate)
                      button(
                        cls := "btn btn-primary",
                        i(cls := "cil-plus me-1"),
                        emptyCta,
                        onClick --> { _ => viewVar.set(PanelView.CreateForm) },
                      )
                    else emptyNode,
                  )
                case _ =>
                  renderTable(entriesSignal, () => onRefresh())
              },
            )
        },
      ),
    )
  }
}

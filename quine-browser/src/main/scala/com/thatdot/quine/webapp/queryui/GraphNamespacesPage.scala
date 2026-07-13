package com.thatdot.quine.webapp.queryui

import scala.concurrent.Future
import scala.util.{Failure, Success}

import com.raquo.laminar.api.L._
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.webapp.Styles

object GraphNamespacesPage {

  private val PageCss =
    """.graph-ns-delete { font-size: 1.25rem; color: var(--cui-danger, #e55353); cursor: pointer; }
      |.graph-ns-delete:hover { color: var(--cui-danger-emphasis, #b02a37); }""".stripMargin

  /** Render the Graph Namespaces page.
    *
    * @param canCreate whether the user may create graphs; hides the create form when false
    * @param canDelete whether the user may delete graphs; hides the delete actions when false
    * @param onCreateNamespace create the named graph, returning a user-facing error message on failure
    * @param onDeleteNamespace delete the named graph, returning a user-facing error message on failure
    */
  def apply(
    knownNamespaces: Signal[Seq[String]],
    selectedNamespace: Signal[Option[String]],
    canCreate: Boolean,
    canDelete: Boolean,
    onCreateNamespace: String => Future[Either[String, Unit]],
    onDeleteNamespace: String => Future[Either[String, Unit]],
    onRefreshNamespaces: () => Unit,
  ): HtmlElement = {
    val searchVar = Var("")
    val errorVar = Var(Option.empty[String])
    val createFormOpenVar = Var(false)
    val createNameVar = Var("")
    val pendingDeleteVar = Var(Option.empty[String])
    val requestInFlightVar = Var(false)

    def errorMessage(err: Throwable): String =
      Option(err.getMessage).filter(_.nonEmpty).getOrElse("network error")

    def submitCreate(): Unit = {
      val name = createNameVar.now().trim
      if (name.nonEmpty && !requestInFlightVar.now()) {
        requestInFlightVar.set(true)
        onCreateNamespace(name).onComplete { result =>
          requestInFlightVar.set(false)
          result match {
            case Success(Right(())) =>
              errorVar.set(None)
              createNameVar.set("")
              createFormOpenVar.set(false)
            case Success(Left(message)) =>
              errorVar.set(Some(message))
            case Failure(err) =>
              errorVar.set(Some(s"Failed to create graph: ${errorMessage(err)}"))
          }
        }
      }
    }

    def submitDelete(ns: String): Unit =
      if (!requestInFlightVar.now()) {
        requestInFlightVar.set(true)
        onDeleteNamespace(ns).onComplete { result =>
          requestInFlightVar.set(false)
          pendingDeleteVar.set(None)
          result match {
            case Success(Right(())) => errorVar.set(None)
            case Success(Left(message)) => errorVar.set(Some(message))
            case Failure(err) => errorVar.set(Some(s"Failed to delete graph: ${errorMessage(err)}"))
          }
        }
      }

    val createForm = div(
      cls := "mb-3",
      div(
        cls := "d-flex gap-2 align-items-center",
        input(
          typ := "text",
          cls := "form-control form-control-sm",
          maxWidth := "24em",
          placeholder := "Graph name",
          controlled(value <-- createNameVar, onInput.mapToValue --> createNameVar),
          onKeyDown.filter(_.key == "Enter") --> { _ => submitCreate() },
          onMountCallback(_.thisNode.ref.focus()),
        ),
        button(
          cls := "btn btn-primary btn-sm",
          disabled <-- createNameVar.signal.combineWith(requestInFlightVar.signal).map { case (name, busy) =>
            name.trim.isEmpty || busy
          },
          "Create",
          onClick --> { _ => submitCreate() },
        ),
        button(
          cls := "btn btn-secondary btn-sm",
          "Cancel",
          onClick --> { _ =>
            createNameVar.set("")
            createFormOpenVar.set(false)
          },
        ),
      ),
      div(
        cls := "form-text",
        "1-16 characters, starting with a letter, letters and digits only.",
      ),
    )

    def rowActions(ns: String, selected: Option[String], pendingDelete: Option[String]): Node =
      if (!canDelete || selected.contains(ns)) emptyNode
      else
        pendingDelete match {
          case Some(pending) if pending == ns =>
            span(
              cls := "d-inline-flex gap-2 align-items-center",
              span(cls := "text-danger", s"""Delete "$ns" and all of its data?"""),
              button(
                cls := "btn btn-danger btn-sm",
                disabled <-- requestInFlightVar.signal,
                "Delete",
                onClick --> { e =>
                  e.stopPropagation()
                  submitDelete(ns)
                },
              ),
              button(
                cls := "btn btn-secondary btn-sm",
                "Cancel",
                onClick --> { e =>
                  e.stopPropagation()
                  pendingDeleteVar.set(None)
                },
              ),
            )
          case _ =>
            htmlTag("i")(
              cls := "graph-ns-delete cil-trash",
              title := s"Delete $ns",
              onClick --> { e =>
                e.stopPropagation()
                pendingDeleteVar.set(Some(ns))
              },
            )
        }

    div(
      cls := "container-fluid px-3",
      htmlTag("style")(PageCss),
      onMountCallback(_ => onRefreshNamespaces()),
      EventStream.periodic(intervalMs = 10000).mapTo(()) --> Observer[Unit](_ => onRefreshNamespaces()),
      div(
        cls := "d-flex align-items-center",
        height := "var(--cui-sidebar-header-height, 4rem)",
        h2(cls := "h2 mb-0 px-3", "Graph Namespaces"),
      ),
      div(
        cls := "card",
        div(
          cls := "card-header d-flex justify-content-between align-items-center",
          h5(cls := "mb-0", "Graphs"),
          if (canCreate)
            child <-- createFormOpenVar.signal.map { open =>
              if (open) emptyNode
              else
                button(
                  cls := "btn btn-primary btn-sm",
                  i(cls := "cil-plus me-1"),
                  "New Graph",
                  onClick --> { _ =>
                    errorVar.set(None)
                    createFormOpenVar.set(true)
                  },
                )
            }
          else emptyNode,
        ),
        div(
          cls := "card-body",
          child <-- errorVar.signal.map {
            case None => emptyNode
            case Some(message) =>
              div(
                cls := "alert alert-danger alert-dismissible mb-3",
                message,
                button(
                  typ := "button",
                  cls := "btn-close",
                  onClick --> { _ => errorVar.set(None) },
                ),
              )
          },
          child <-- createFormOpenVar.signal.map(open => if (open) createForm else emptyNode),
          div(
            cls := Styles.managerSearch,
            htmlTag("i")(cls := "ion-ios-search"),
            input(
              typ := "text",
              placeholder := "Search graphs…",
              controlled(value <-- searchVar, onInput.mapToValue --> searchVar),
            ),
          ),
          child <-- knownNamespaces
            .combineWith(searchVar.signal, selectedNamespace, pendingDeleteVar.signal)
            .map { case (namespaces, search, selected, pendingDelete) =>
              val needle = search.trim.toLowerCase
              val defaultNs = NamespaceParameter.defaultNamespaceParameter.namespaceId
              val filtered =
                (if (needle.isEmpty) namespaces
                 else namespaces.filter(_.toLowerCase.contains(needle)))
                  .sortBy(n => (!n.equalsIgnoreCase(defaultNs), n.toLowerCase))
              if (filtered.isEmpty)
                div(cls := Styles.managerListEmpty, "No graphs match your search.")
              else
                div(
                  cls := Styles.managerList,
                  filtered.map { ns =>
                    div(
                      cls := Styles.managerListItem,
                      div(
                        cls := Styles.managerListItemMain,
                        span(cls := Styles.managerListItemName, ns),
                      ),
                      div(
                        cls := Styles.managerListItemActions,
                        rowActions(ns, selected, pendingDelete),
                      ),
                    )
                  },
                )
            },
          div(
            cls := Styles.managerFooter,
            child.text <-- knownNamespaces.map(ns => s"${ns.size} ${if (ns.size == 1) "graph" else "graphs"}"),
          ),
        ),
      ),
    )
  }
}

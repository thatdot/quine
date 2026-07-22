package com.thatdot.quine.webapp.queryui

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

/** Top-bar configuration "junk drawer" — a gear trigger showing the active graph's name, opening
  * a sectioned popover: **Graph** (the host's [[GraphSelector]] content, promoted to the top so
  * switching graphs — the common action — is first-class), **Configure** (opens the Explorer
  * Settings modal; this just emits a callback), and **Maintenance** (clear canvas /
  * reset persisted state — relocated from the old top-bar reload icon).
  *
  * Host-agnostic: every side effect is a callback or an already-built element the caller
  * supplies (e.g. the `graphSection` param is the host's constructed `GraphSelector(...)`
  * element or an equivalent — this component never touches namespace/selection logic itself).
  * Same outside-click/Escape-close idiom as [[GraphSelector]] and the results switcher.
  */
object JunkDrawer {

  /** @param activeGraphName the active graph/namespace name shown on the trigger
    * @param graphSection the Graph section's body — typically the host's `GraphSelector(...)`
    *   element (or its list rendered inline); this component only provides the labeled
    *   section frame around it. `None` (hosts without a graph selector, e.g. community
    *   Quine and Novelty) omits the Graph section entirely — the Configure and
    *   Maintenance sections still render
    * @param showExplorerSettings whether the Configure section (the "Explorer settings…"
    *   entry) renders at all — hosts hide it for users lacking the settings write
    *   permissions, the same gate the old settings nav page used
    * @param multiNamespace whether the host can have more than one namespace in this tab.
    *   Distinct from `graphSection`: enterprise has a graph selector *in* the drawer,
    *   Novelty switches namespaces (its "Models") from a dropdown *outside* QueryUi — both
    *   are multi-namespace. Only OSS is single-namespace; there the Maintenance section
    *   collapses to one plain "Clear canvas" entry with no namespace wording and no
    *   all-namespaces purge
    * @param onOpenExplorerSettings "Explorer settings…" entry in Configure — opens the
    *   host's Explorer Settings modal; this only emits
    * @param onClearNamespace "Clear canvas (this namespace)" in Maintenance — the relocated
    *   reload-icon action; clears the canvas (nodes/edges/results) and deletes persisted
    *   browser session storage for the active namespace only. On single-namespace hosts
    *   this is the sole Maintenance entry, labeled just "Clear canvas"
    * @param onResetAllNamespaces "Clear canvas (all namespaces)" in Maintenance — clears the
    *   canvas and deletes persisted browser state for every namespace in this tab. Only
    *   offered when `multiNamespace`; single-namespace hosts never see it
    * @param closeOn external close requests — e.g. the host emits on namespace change so
    *   selecting a graph in the (host-provided, otherwise opaque) Graph section dismisses the
    *   drawer like a menu selection would
    */
  def apply(
    activeGraphName: Signal[String],
    graphSection: Option[HtmlElement],
    multiNamespace: Boolean = true,
    showExplorerSettings: Boolean = true,
    onOpenExplorerSettings: () => Unit,
    onClearNamespace: () => Unit,
    onResetAllNamespaces: () => Unit,
    closeOn: EventStream[Unit] = EventStream.empty,
  ): HtmlElement = {
    val openVar = Var(false)
    var rootEl: Option[dom.html.Element] = None

    def close(): Unit = openVar.set(false)
    def toggle(): Unit = openVar.update(!_)

    def runAndClose(action: () => Unit): Observer[dom.MouseEvent] =
      Observer { _ =>
        close()
        action()
      }

    // Outside-click / Escape dismissal — same idiom as GraphSelector and QueryMenuButton (a
    // plain document listener pair registered on mount, torn down on unmount).
    val handleMouseDown: js.Function1[dom.MouseEvent, Unit] = { (event: dom.MouseEvent) =>
      if (openVar.now()) {
        val target = event.target.asInstanceOf[dom.Node]
        val inside = rootEl.exists(_.contains(target))
        if (!inside) close()
      }
    }
    val handleKeyDown: js.Function1[dom.KeyboardEvent, Unit] = { (event: dom.KeyboardEvent) =>
      if (openVar.now() && event.key == "Escape") close()
    }

    div(
      cls := JunkDrawerStyles.junkDrawer,
      closeOn --> (_ => close()),
      onMountCallback { ctx =>
        rootEl = Some(ctx.thisNode.ref)
        dom.document.addEventListener("mousedown", handleMouseDown)
        dom.document.addEventListener("keydown", handleKeyDown)
      },
      onUnmountCallback { _ =>
        rootEl = None
        dom.document.removeEventListener("mousedown", handleMouseDown)
        dom.document.removeEventListener("keydown", handleKeyDown)
      },
      button(
        tpe := "button",
        cls := JunkDrawerStyles.junkDrawerTrigger,
        // Hosts without a graph selector in the drawer show no name on the trigger: OSS
        // has only the default namespace (its name, "quine", would read as branding), and
        // Novelty already shows the active Model in its own dropdown outside QueryUi.
        graphSection match {
          case Some(_) =>
            Seq[Modifier[HtmlElement]](
              title <-- activeGraphName.map(name => s"Graph: $name, settings & maintenance"),
              span(cls := JunkDrawerStyles.junkDrawerTriggerName, child.text <-- activeGraphName),
            )
          case None => Seq[Modifier[HtmlElement]](title := "Settings & maintenance")
        },
        aria.hasPopup := true,
        aria.expanded <-- openVar.signal,
        onClick --> (_ => toggle()),
        i(cls := s"cil-cog ${JunkDrawerStyles.junkDrawerTriggerIcon}"),
      ),
      div(
        cls <-- openVar.signal.map(open =>
          s"${JunkDrawerStyles.junkDrawerMenu}${if (open) s" ${JunkDrawerStyles.junkDrawerMenuOpen}" else ""}",
        ),
        children <-- openVar.signal.map {
          case false => Seq.empty[HtmlElement]
          case true =>
            graphSection.toSeq.flatMap { graphEl =>
              Seq(
                section("Graph", div(cls := JunkDrawerStyles.junkDrawerGraphSlot, graphEl)),
                separator(),
              )
            } ++ (if (showExplorerSettings)
                    Seq(
                      section(
                        "Configure",
                        item("cil-settings", "Exploration UI Settings", runAndClose(() => onOpenExplorerSettings())),
                      ),
                      separator(),
                    )
                  else Seq.empty) ++ Seq(
              // Single-namespace hosts (OSS) get a single plain "Clear canvas" —
              // namespace-scoped wording and the all-namespaces purge would name a
              // distinction that doesn't exist there. Note this keys on `multiNamespace`,
              // not `graphSection`: Novelty is multi-namespace with its selector outside
              // the drawer.
              section(
                "Maintenance",
                if (multiNamespace)
                  Seq(
                    item(
                      "cil-trash",
                      "Clear canvas (this namespace)",
                      runAndClose(() => onClearNamespace()),
                      danger = false,
                      tooltip = Some(
                        "Removes saved node positions, pinned nodes, and query history for this " +
                        "namespace's canvas. Stored only in this browser, can't be undone.",
                      ),
                    ),
                    item(
                      "cil-warning",
                      "Clear canvas (all namespaces)",
                      runAndClose(() => onResetAllNamespaces()),
                      danger = true,
                      tooltip = Some(
                        "Removes saved node positions, pinned nodes, and query history for every " +
                        "namespace's canvas. Stored only in this browser, can't be undone.",
                      ),
                    ),
                  )
                else
                  Seq(
                    item(
                      "cil-trash",
                      "Clear canvas",
                      runAndClose(() => onClearNamespace()),
                      danger = false,
                      tooltip = Some(
                        "Removes saved node positions, pinned nodes, and query history for the " +
                        "canvas. Stored only in this browser, can't be undone.",
                      ),
                    ),
                  ),
              ),
            )
        },
      ),
    )
  }

  private def separator(): HtmlElement = div(cls := JunkDrawerStyles.junkDrawerSeparator)

  private def section(label: String, content: Modifier[HtmlElement]*): HtmlElement =
    div(
      cls := JunkDrawerStyles.junkDrawerSection,
      div(cls := JunkDrawerStyles.junkDrawerSectionHead, span(cls := JunkDrawerStyles.junkDrawerSectionLabel, label)),
      div(cls := JunkDrawerStyles.junkDrawerSectionBody, content),
    )

  private def item(
    iconClass: String,
    label: String,
    onSelect: Observer[dom.MouseEvent],
    danger: Boolean = false,
    tooltip: Option[String] = None,
  ): HtmlElement =
    div(
      cls := JunkDrawerStyles.junkDrawerItem,
      cls(JunkDrawerStyles.junkDrawerItemDanger) := danger,
      tooltip.map(title := _),
      onClick --> onSelect,
      i(cls := s"$iconClass ${JunkDrawerStyles.junkDrawerItemIcon}"),
      span(cls := JunkDrawerStyles.junkDrawerItemLabel, label),
    )
}

package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.QueryEditorStyles

/** The severity of an LSP diagnostic, parsed from the editor package's
  * [[EditorDiagnostic.severity]] string at the construction edge so the render model is typed
  * rather than stringly-compared. Mirrors the four LSP severities; `info` and `hint` share the
  * info presentation.
  */
sealed abstract class DiagnosticSeverity
object DiagnosticSeverity {
  case object Error extends DiagnosticSeverity
  case object Warning extends DiagnosticSeverity
  case object Info extends DiagnosticSeverity
  case object Hint extends DiagnosticSeverity

  /** Parses an [[EditorDiagnostic.severity]] string; an unrecognized value defaults to [[Info]]
    * (the least-alarming non-actionable level) rather than throwing.
    */
  def fromString(wire: String): DiagnosticSeverity = wire match {
    case "error" => Error
    case "warning" => Warning
    case "hint" => Hint
    case _ => Info
  }

  /** The diagnostic-item CSS class driving the colored left border; `info`/`hint` share a color. */
  def cssClass(severity: DiagnosticSeverity): String = severity match {
    case Error => QueryEditorStyles.queryDiagnosticError
    case Warning => QueryEditorStyles.queryDiagnosticWarning
    case Info | Hint => QueryEditorStyles.queryDiagnosticInfo
  }
}

/** Shared status bar + toggleable diagnostics panel for the Monaco query editors, driven by the
  * editor package's `onDiagnosticsListChange` callback. Mounted below the editor by both the
  * nav-bar Query input (`MonacoQueryInput`, inside its expanded chrome) and the Streams form's embedded
  * editor (`EmbeddedQueryEditor`).
  *
  * The status bar shows a problems summary (`No problems` / `2 Errors  1 Warning`) and toggles the
  * panel on click. The panel lists each diagnostic (severity color, message, `Ln/Col`); clicking a
  * row jumps the editor's cursor to that position via the `revealRange` callback.
  *
  * Both the bar and the panel guard mousedown with `preventDefault` so a click keeps the editor's
  * focus (the nav-bar editor collapses its overlay on blur; this stops the click from pulling the
  * chrome out from under itself).
  */
object QueryDiagnosticsView {

  /** Render model for a single LSP diagnostic. Decoupled from the native [[EditorDiagnostic]] JS
    * trait so the reactive state holds plain Scala values. `position` is the 1-based range start,
    * matching Monaco.
    */
  final case class QueryDiagnostic(message: String, severity: DiagnosticSeverity, position: Position)

  /** @param diagnostics current diagnostics on the editor's model
    * @param showDiagnostics whether the panel is expanded; toggled by clicking the status bar
    * @param revealRange jump the editor's cursor to a [[Position]]; wired to the editor handle's
    *   `revealRange` so clicking a diagnostic row focuses the editor at that position
    */
  def apply(
    diagnostics: Signal[Seq[QueryDiagnostic]],
    showDiagnostics: Var[Boolean],
    revealRange: Position => Unit,
  ): HtmlElement =
    div(
      statusBar(diagnostics, showDiagnostics),
      diagnosticsPanel(diagnostics, showDiagnostics, revealRange),
    )

  private def statusBar(
    diagnostics: Signal[Seq[QueryDiagnostic]],
    showDiagnostics: Var[Boolean],
  ): HtmlElement =
    div(
      cls := QueryEditorStyles.queryStatusBar,
      onMouseDown --> (_.preventDefault()), // keep editor focus
      onClick --> (_ => showDiagnostics.update(!_)),
      span(
        child.text <-- diagnostics.map { ds =>
          val e = ds.count(_.severity == DiagnosticSeverity.Error)
          val w = ds.count(_.severity == DiagnosticSeverity.Warning)
          if (e == 0 && w == 0) "No problems"
          else
            Seq(
              Option.when(e > 0)(s"$e ${plural(e, "Error")}"),
              Option.when(w > 0)(s"$w ${plural(w, "Warning")}"),
            ).flatten.mkString("  ")
        },
      ),
      span(
        cls <-- showDiagnostics.signal.map(s =>
          if (s) QueryEditorStyles.queryStatusChevronOpen else QueryEditorStyles.queryStatusChevron,
        ),
      ),
    )

  private def diagnosticsPanel(
    diagnostics: Signal[Seq[QueryDiagnostic]],
    showDiagnostics: Var[Boolean],
    revealRange: Position => Unit,
  ): HtmlElement =
    div(
      cls := QueryEditorStyles.queryDiagnosticsPanel,
      onMouseDown --> (_.preventDefault()),
      display <-- showDiagnostics.signal.combineWith(diagnostics).map { case (open, ds) =>
        if (open && ds.nonEmpty) "" else "none"
      },
      children <-- diagnostics.map(_.map(d => diagnosticRow(d, revealRange))),
    )

  /** A single diagnostic row: severity-colored left border (via [[DiagnosticSeverity.cssClass]]),
    * the message, and a monospace line:column locator. Clicking the row jumps the editor cursor
    * to the diagnostic's position (and focuses the editor).
    */
  private def diagnosticRow(d: QueryDiagnostic, revealRange: Position => Unit): HtmlElement =
    div(
      cls := s"${QueryEditorStyles.queryDiagnosticItem} ${DiagnosticSeverity.cssClass(d.severity)}",
      onClick --> (_ => revealRange(d.position)),
      span(cls := QueryEditorStyles.queryDiagnosticMessage, d.message),
      span(cls := QueryEditorStyles.queryDiagnosticPos, s"Ln ${d.position.line}, Col ${d.position.column}"),
    )

  /** Singular/plural word for a diagnostic count, e.g. `plural(1, "Error") == "Error"`,
    * `plural(2, "Error") == "Errors"`.
    */
  private def plural(n: Int, word: String): String = if (n == 1) word else s"${word}s"
}

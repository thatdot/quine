package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._
import io.circe.Json

import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2IngestInfo

/** Renders the ingest streams table with status badges and action icons.
  *
  * Pure renderer: receives Signals to read, Observers to write. No API
  * knowledge — the parent wires observers to API calls.
  */
object IngestStreamTable {

  /** The action observers carry the resource's `(name, memberIdx)` so the parent can
    * route the mutation to the cluster member running that ingest.
    *
    * @param memberIndices known cluster member positions; empty on single-node deployments
    */
  def apply(
    entriesSignal: Signal[List[(String, V2IngestInfo)]],
    memberIndices: Signal[Seq[Int]],
    canControl: Boolean,
    canDelete: Boolean,
    onDelete: Observer[(String, Option[Int])],
    onPause: Observer[(String, Option[Int])],
    onResume: Observer[(String, Option[Int])],
  ): HtmlElement = {
    val expandedVar: Var[Set[String]] = Var(Set.empty)
    // Gate the Member column on actual cluster membership — the same gate as the
    // create-form's position selector, so the column and selector appear together.
    val showMember: Signal[Boolean] = memberIndices.map(_.nonEmpty).distinct
    table(
      cls := "table table-hover mb-0",
      thead(
        tr(
          th(styleAttr := "width: 40px"),
          th("Name"),
          child <-- showMember.map(if (_) th("Member") else emptyNode),
          th("Type"),
          th("Status"),
          th("Ingested"),
          th("Rate (1m)"),
          th("Uptime"),
          th("Actions"),
        ),
      ),
      children <-- entriesSignal.splitSeq(_._1) { strictSignal =>
        val key = strictSignal.key
        val infoSignal = strictSignal.map(_._2)
        val isExpanded = expandedVar.signal.map(_.contains(key)).distinct
        tbody(
          renderRow(
            key,
            infoSignal,
            showMember,
            isExpanded,
            expandedVar,
            canControl,
            canDelete,
            onDelete,
            onPause,
            onResume,
          ),
          renderExpandedRow(infoSignal, showMember, isExpanded),
        )
      },
    )
  }

  private def renderRow(
    key: String,
    infoSignal: Signal[V2IngestInfo],
    showMember: Signal[Boolean],
    isExpanded: Signal[Boolean],
    expandedVar: Var[Set[String]],
    canControl: Boolean,
    canDelete: Boolean,
    onDelete: Observer[(String, Option[Int])],
    onPause: Observer[(String, Option[Int])],
    onResume: Observer[(String, Option[Int])],
  ): HtmlElement = {
    val nameSignal = infoSignal.map(_.name)
    val memberSignal = infoSignal.map(_.memberIdx)
    val statusSignal = infoSignal.map(_.status)
    val messageSignal = infoSignal.map(_.message.filter(_.nonEmpty))
    val sourceTypeSignal = infoSignal.map(_.sourceType)
    val statsSignal = infoSignal.map(_.stats)

    tr(
      cls <-- statusSignal.map(s => if (s == "Failed") "table-danger" else ""),
      td(
        button(
          cls := "btn btn-sm btn-ghost-secondary p-0",
          child <-- isExpanded.map { exp =>
            if (exp) i(cls := "cil-chevron-bottom") else i(cls := "cil-chevron-right")
          },
          onClick --> { _ =>
            expandedVar.update(exp => if (exp.contains(key)) exp - key else exp + key)
          },
        ),
      ),
      td(child.text <-- nameSignal),
      child <-- showMember.map(if (_) td(child.text <-- memberSignal.map(_.fold("—")(_.toString))) else emptyNode),
      td(
        child <-- sourceTypeSignal.map { st =>
          span(
            cls := "d-inline-flex align-items-center",
            IngestSourceIcons.forSourceType(st, st),
            code(st),
          )
        },
      ),
      td(child <-- statusSignal.map(statusBadge)),
      // Stats cells — show error message spanning these columns when failed
      children <-- statusSignal.combineWith(messageSignal, statsSignal).map {
        case ("Failed", message, _) =>
          List(
            td(
              colSpan := 3,
              small(
                cls := "text-danger",
                i(cls := "cil-warning me-1"),
                message.getOrElse[String]("Unknown error"),
              ),
            ),
          )
        case (_, _, stats) =>
          val ingestedCount = formatCount(stats.ingestedCount)
          val rate = f"${stats.rates.oneMinute}%.1f/s"
          val uptime = stats.totalRuntime.filter(_.nonEmpty).map(formatUptime).getOrElse("-")
          List(td(ingestedCount), td(rate), td(uptime))
      },
      // Actions cell
      td(
        cls := "text-nowrap",
        child <-- statusSignal.combineWith(nameSignal, memberSignal).map { case (status, name, memberIdx) =>
          val target = (name, memberIdx)
          val isRunning = status == "Running"
          val isResumable = Set("Paused", "Restored").contains(status)
          val isFailed = status == "Failed"
          val deleteButton = Option.when(canDelete)(
            button(
              cls := "btn btn-sm btn-ghost-danger",
              title := "Delete",
              i(cls := "cil-trash"),
              onClick --> { _ => onDelete.onNext(target) },
            ),
          )
          val controlButtons = Option.when(!isFailed && canControl)(
            List(
              button(
                cls := "btn btn-sm btn-ghost-success me-1",
                title := "Resume",
                disabled := !isResumable,
                i(cls := "cil-media-play"),
                onClick --> { _ => onResume.onNext(target) },
              ),
              button(
                cls := "btn btn-sm btn-ghost-warning me-1",
                title := "Pause",
                disabled := !isRunning,
                i(cls := "cil-media-pause"),
                onClick --> { _ => onPause.onNext(target) },
              ),
            ),
          )
          span(controlButtons.toList.flatten ++ deleteButton.toList)
        },
      ),
    )
  }

  private val VolatileFields = Set("stats", "status", "message")

  private def configOnly(json: Json): Json =
    json.asObject.fold(json)(obj => Json.fromJsonObject(obj.filterKeys(k => !VolatileFields.contains(k))))

  private def liveOnly(json: Json): Json =
    json.asObject.fold(json)(obj => Json.fromJsonObject(obj.filterKeys(VolatileFields.contains)))

  private def renderExpandedRow(
    infoSignal: Signal[V2IngestInfo],
    showMember: Signal[Boolean],
    isExpanded: Signal[Boolean],
  ): HtmlElement =
    tr(
      cls := "bg-body-tertiary",
      display <-- isExpanded.map(if (_) "table-row" else "none"),
      td(
        colSpan <-- showMember.map(if (_) 9 else 8),
        div(
          cls := "ms-4 py-2",
          strong("Configuration"),
          pre(
            cls := "mb-0 mt-1 p-2 bg-body rounded border",
            styleAttr := "max-height: 24em; overflow: auto; font-size: 0.85em;",
            child.text <-- infoSignal.map(info => configOnly(info.raw).spaces2).distinct,
          ),
          div(
            cls := "mt-2",
            strong("Live Stats"),
            pre(
              cls := "mb-0 mt-1 p-2 bg-body rounded border",
              styleAttr := "max-height: 24em; overflow: auto; font-size: 0.85em;",
              child.text <-- infoSignal.map(info => liveOnly(info.raw).spaces2),
            ),
          ),
        ),
      ),
    )

  private def statusBadge(status: String): HtmlElement = {
    val badgeClass = status match {
      case "Running" => "badge bg-success"
      case "Paused" | "Restored" => "badge bg-warning text-dark"
      case "Failed" => "badge bg-danger"
      case "Completed" | "Terminated" => "badge bg-secondary"
      case _ => "badge bg-secondary"
    }
    span(cls := badgeClass, status)
  }

  private def formatCount(n: Long): String =
    if (n >= 1000000) f"${n / 1000000.0}%.1fM"
    else if (n >= 1000) f"${n / 1000.0}%.1fK"
    else n.toString

  private val DurationSegment = """(\d+(?:\.\d+)?)(ms|µs|us|ns|h|m|s)""".r

  private def formatUptime(raw: String): String = {
    val segments = DurationSegment.findAllMatchIn(raw).toList
    if (segments.isEmpty) return raw

    var totalSeconds = 0L
    segments.foreach { m =>
      val value = m.group(1).toDouble
      m.group(2) match {
        case "h" => totalSeconds += (value * 3600).toLong
        case "m" => totalSeconds += (value * 60).toLong
        case "s" => totalSeconds += value.toLong
        case _ => // drop sub-second
      }
    }

    if (totalSeconds <= 0) return "< 1s"

    val days = totalSeconds / 86400
    val hours = (totalSeconds % 86400) / 3600
    val minutes = (totalSeconds % 3600) / 60
    val seconds = totalSeconds % 60

    val parts = Seq(
      if (days > 0) Some(s"${days}d") else None,
      if (hours > 0) Some(s"${hours}h") else None,
      if (minutes > 0) Some(s"${minutes}m") else None,
      if (days == 0 && seconds > 0) Some(s"${seconds}s") else None,
    ).flatten

    parts.mkString(" ")
  }
}

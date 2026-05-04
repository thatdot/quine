package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._
import io.circe.Json

import com.thatdot.quine.webapp.components.landing.V2ApiTypes.V2IngestInfo

/** Renders the ingest streams table with status badges and action icons.
  *
  * Pure renderer: receives Signals to read, Observers to write. No API
  * knowledge — the parent wires observers to API calls.
  */
object IngestStreamTable {

  def apply(
    entriesSignal: Signal[List[(String, Json)]],
    onDelete: Observer[String],
    onPause: Observer[String],
    onResume: Observer[String],
  ): HtmlElement =
    table(
      cls := "table table-hover mb-0",
      thead(
        tr(
          th("Name"),
          th("Type"),
          th("Status"),
          th("Ingested"),
          th("Rate (1m)"),
          th("Uptime"),
          th("Actions"),
        ),
      ),
      tbody(
        children <-- entriesSignal.splitSeq(_._1) { strictSignal =>
          renderRow(strictSignal.key, strictSignal.map(_._2), onDelete, onPause, onResume)
        },
      ),
    )

  private def renderRow(
    name: String,
    jsonSignal: Signal[Json],
    onDelete: Observer[String],
    onPause: Observer[String],
    onResume: Observer[String],
  ): HtmlElement = {
    val statusSignal = jsonSignal.map { json =>
      json.hcursor.get[String]("status").toOption.map(V2IngestInfo.humanizeStatus).getOrElse("Unknown")
    }
    val messageSignal = jsonSignal.map { json =>
      json.hcursor.get[String]("message").toOption.filter(_.nonEmpty)
    }
    val sourceTypeSignal = jsonSignal.map { json =>
      json.hcursor
        .downField("settings")
        .downField("source")
        .get[String]("type")
        .toOption
        .getOrElse("?")
    }
    val statsSignal = jsonSignal.map(_.hcursor.downField("stats").focus.getOrElse(Json.obj()))

    tr(
      cls <-- statusSignal.map(s => if (s == "Failed") "table-danger" else ""),
      td(name),
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
          val ingestedCount = stats.hcursor
            .get[Long]("ingestedCount")
            .toOption
            .map(formatCount)
            .getOrElse("-")
          val rate = stats.hcursor
            .downField("rates")
            .get[Double]("oneMinute")
            .toOption
            .map(r => f"$r%.1f/s")
            .getOrElse("-")
          val uptime = stats.hcursor
            .get[String]("totalRuntime")
            .toOption
            .filter(_.nonEmpty)
            .map(formatUptime)
            .getOrElse("-")
          List(td(ingestedCount), td(rate), td(uptime))
      },
      // Actions cell
      td(
        cls := "text-nowrap",
        child <-- statusSignal.map { status =>
          val isRunning = status == "Running"
          val isResumable = Set("Paused", "Restored").contains(status)
          val isFailed = status == "Failed"
          if (isFailed)
            span(
              button(
                cls := "btn btn-sm btn-ghost-danger",
                title := "Delete",
                i(cls := "cil-trash"),
                onClick --> { _ => onDelete.onNext(name) },
              ),
            )
          else
            span(
              button(
                cls := "btn btn-sm btn-ghost-success me-1",
                title := "Resume",
                disabled := !isResumable,
                i(cls := "cil-media-play"),
                onClick --> { _ => onResume.onNext(name) },
              ),
              button(
                cls := "btn btn-sm btn-ghost-warning me-1",
                title := "Pause",
                disabled := !isRunning,
                i(cls := "cil-media-pause"),
                onClick --> { _ => onPause.onNext(name) },
              ),
              button(
                cls := "btn btn-sm btn-ghost-danger",
                title := "Delete",
                i(cls := "cil-trash"),
                onClick --> { _ => onDelete.onNext(name) },
              ),
            )
        },
      ),
    )
  }

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

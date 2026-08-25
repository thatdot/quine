package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._
import io.circe.Json

import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2IngestInfo

/** Renders the ingest streams table with status badges and action icons.
  *
  * Rows are collapsed by ingest name: a clustered ingest running on several
  * members appears once, with summed stats and a "Mixed" status when its members
  * disagree. A single-member group keeps its inline actions and behaves exactly
  * like a non-clustered ingest; a multi-member group defers per-member actions to
  * the expanded breakdown, so a click is never ambiguous about which member it hits.
  *
  * Pure renderer: receives Signals to read, Observers to write. No API
  * knowledge — the parent wires observers to API calls.
  */
object IngestStreamTable {

  /** Displayed in place of a group-level status or source type when its members disagree. */
  private val Mixed = "Mixed"

  /** The action observers carry the resource's `(name, memberIdx)` so the parent can
    * route the mutation to the cluster member running that ingest.
    *
    * @param memberIndices known cluster member positions; empty on single-node deployments
    */
  def apply(
    entriesSignal: Signal[List[(String, IngestGroup)]],
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
        val groupSignal = strictSignal.map(_._2)
        val isExpanded = expandedVar.signal.map(_.contains(key)).distinct
        tbody(
          renderRow(
            key,
            groupSignal,
            showMember,
            isExpanded,
            expandedVar,
            canControl,
            canDelete,
            onDelete,
            onPause,
            onResume,
          ),
          renderExpandedRow(
            groupSignal,
            showMember,
            isExpanded,
            canControl,
            canDelete,
            onDelete,
            onPause,
            onResume,
          ),
        )
      },
    )
  }

  /** The collapsed, name-level row. Stats are summed across members; a single-member
    * group keeps inline actions, a multi-member group leaves the actions cell empty
    * (its actions live in the per-member breakdown of the expanded row).
    */
  private def renderRow(
    key: String,
    groupSignal: Signal[IngestGroup],
    showMember: Signal[Boolean],
    isExpanded: Signal[Boolean],
    expandedVar: Var[Set[String]],
    canControl: Boolean,
    canDelete: Boolean,
    onDelete: Observer[(String, Option[Int])],
    onPause: Observer[(String, Option[Int])],
    onResume: Observer[(String, Option[Int])],
  ): HtmlElement = {
    val nameSignal = groupSignal.map(_.name)
    val memberLabelSignal = groupSignal.map(_.memberIndices.mkString(", "))
    val statusSignal = groupSignal.map(_.agreedStatus)
    val sourceTypeSignal = groupSignal.map(_.agreedSourceType)

    tr(
      cls <-- statusSignal.map(s => if (s.contains("Failed")) "table-danger" else ""),
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
      child <-- showMember.map(
        if (_) td(child.text <-- memberLabelSignal.map(l => if (l.isEmpty) "—" else l)) else emptyNode,
      ),
      td(
        child <-- sourceTypeSignal.map { agreed =>
          val st = agreed.getOrElse(Mixed)
          span(
            cls := "d-inline-flex align-items-center",
            IngestSourceIcons.forSourceType(st, st),
            code(st),
          )
        },
      ),
      td(child <-- statusSignal.map(agreed => statusBadge(agreed.getOrElse(Mixed)))),
      // Stats cells — summed across members; show a combined error when all members failed.
      children <-- groupSignal.map(groupStatCells),
      // Actions cell — inline only for a single-member group; multi-member groups act per
      // member from the expanded breakdown so a click can't silently hit the wrong member.
      td(
        cls := "text-nowrap",
        child <-- groupSignal.map { g =>
          if (g.size == 1) {
            val info = g.members.head
            actionButtons(info.status, info.name, info.memberIdx, canControl, canDelete, onDelete, onPause, onResume)
          } else emptyNode
        },
      ),
    )
  }

  private val VolatileFields = Set("stats", "status", "message")

  private def configOnly(json: Json): Json =
    json.asObject.fold(json)(obj => Json.fromJsonObject(obj.filterKeys(k => !VolatileFields.contains(k))))

  private def liveOnly(json: Json): Json =
    json.asObject.fold(json)(obj => Json.fromJsonObject(obj.filterKeys(VolatileFields.contains)))

  /** The expanded detail row. A multi-member group shows a per-member breakdown whose
    * own rows expand to each member's configuration and live stats; a single-member
    * group shows that one member's configuration and live stats directly. Config and
    * stats are always attributed to a specific member — never combined across the cluster.
    */
  private def renderExpandedRow(
    groupSignal: Signal[IngestGroup],
    showMember: Signal[Boolean],
    isExpanded: Signal[Boolean],
    canControl: Boolean,
    canDelete: Boolean,
    onDelete: Observer[(String, Option[Int])],
    onPause: Observer[(String, Option[Int])],
    onResume: Observer[(String, Option[Int])],
  ): HtmlElement = {
    // Structural decisions key off size only, so per-poll stat updates don't rebuild the subtree.
    val isMultiMember = groupSignal.map(_.size > 1).distinct
    tr(
      cls := "bg-body-tertiary",
      display <-- isExpanded.map(if (_) "table-row" else "none"),
      td(
        colSpan <-- showMember.map(if (_) 9 else 8),
        div(
          cls := "ms-4 py-2",
          child <-- isMultiMember.map {
            if (_)
              div(
                strong("Members"),
                memberBreakdown(groupSignal, canControl, canDelete, onDelete, onPause, onResume),
              )
            else
              // Single member: show its configuration and live stats directly, as a
              // non-clustered ingest would.
              configAndStats(groupSignal.map(_.members.head))
          },
        ),
      ),
    )
  }

  /** The per-member table shown inside a multi-member group's expanded row. Each row
    * expands to that member's own configuration and live stats.
    */
  private def memberBreakdown(
    groupSignal: Signal[IngestGroup],
    canControl: Boolean,
    canDelete: Boolean,
    onDelete: Observer[(String, Option[Int])],
    onPause: Observer[(String, Option[Int])],
    onResume: Observer[(String, Option[Int])],
  ): HtmlElement = {
    val expandedMembers: Var[Set[String]] = Var(Set.empty)
    table(
      cls := "table table-sm mb-0 mt-1",
      thead(
        tr(
          th(styleAttr := "width: 40px"),
          th("Member"),
          th("Status"),
          th("Ingested"),
          th("Rate (1m)"),
          th("Uptime"),
          th("Actions"),
        ),
      ),
      children <-- groupSignal
        .map(_.members)
        .splitSeq(m => m.memberIdx.fold(m.name)(_.toString)) { memberSignal =>
          val memberKey = memberSignal.key
          val memberExpanded = expandedMembers.signal.map(_.contains(memberKey)).distinct
          tbody(
            renderMemberRow(
              memberKey,
              memberSignal,
              memberExpanded,
              expandedMembers,
              canControl,
              canDelete,
              onDelete,
              onPause,
              onResume,
            ),
            renderMemberDetailRow(memberSignal, memberExpanded),
          )
        },
    )
  }

  private def renderMemberRow(
    memberKey: String,
    infoSignal: Signal[V2IngestInfo],
    isExpanded: Signal[Boolean],
    expandedMembers: Var[Set[String]],
    canControl: Boolean,
    canDelete: Boolean,
    onDelete: Observer[(String, Option[Int])],
    onPause: Observer[(String, Option[Int])],
    onResume: Observer[(String, Option[Int])],
  ): HtmlElement =
    tr(
      cls <-- infoSignal.map(i => if (i.status == "Failed") "table-danger" else ""),
      td(
        button(
          cls := "btn btn-sm btn-ghost-secondary p-0",
          child <-- isExpanded.map { exp =>
            if (exp) i(cls := "cil-chevron-bottom") else i(cls := "cil-chevron-right")
          },
          onClick --> { _ =>
            expandedMembers.update(exp => if (exp.contains(memberKey)) exp - memberKey else exp + memberKey)
          },
        ),
      ),
      td(child.text <-- infoSignal.map(_.memberIdx.fold("—")(_.toString))),
      td(child <-- infoSignal.map(i => statusBadge(i.status))),
      children <-- infoSignal.map(memberStatCells),
      td(
        cls := "text-nowrap",
        child <-- infoSignal.map { i =>
          actionButtons(i.status, i.name, i.memberIdx, canControl, canDelete, onDelete, onPause, onResume)
        },
      ),
    )

  /** One member's own configuration and live stats, revealed when its breakdown row is expanded. */
  private def renderMemberDetailRow(
    infoSignal: Signal[V2IngestInfo],
    isExpanded: Signal[Boolean],
  ): HtmlElement =
    tr(
      display <-- isExpanded.map(if (_) "table-row" else "none"),
      td(
        colSpan := 7,
        configAndStats(infoSignal),
      ),
    )

  /** The Configuration + Live Stats blocks for a single member's raw payload. */
  private def configAndStats(infoSignal: Signal[V2IngestInfo]): HtmlElement =
    div(
      strong("Configuration"),
      jsonPre(infoSignal.map(i => configOnly(i.raw).spaces2).distinct),
      div(
        cls := "mt-2",
        strong("Live Stats"),
        jsonPre(infoSignal.map(i => liveOnly(i.raw).spaces2)),
      ),
    )

  private def jsonPre(textSignal: Signal[String]): HtmlElement =
    pre(
      cls := "mb-0 mt-1 p-2 bg-body rounded border",
      styleAttr := "max-height: 24em; overflow: auto; font-size: 0.85em;",
      child.text <-- textSignal,
    )

  /** Resume / pause / delete buttons for one member, gated on permissions and status. */
  private def actionButtons(
    status: String,
    name: String,
    memberIdx: Option[Int],
    canControl: Boolean,
    canDelete: Boolean,
    onDelete: Observer[(String, Option[Int])],
    onPause: Observer[(String, Option[Int])],
    onResume: Observer[(String, Option[Int])],
  ): HtmlElement = {
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
  }

  /** The Ingested / Rate / Uptime cells for a collapsed group: summed counts and rate,
    * the longest member uptime, or a combined error when every member has failed.
    */
  private def groupStatCells(g: IngestGroup): List[HtmlElement] =
    if (g.agreedStatus.contains("Failed"))
      List(errorCell(failureSummary(g)))
    else {
      val ingested = formatCount(g.totalIngestedCount)
      val rate = f"${g.totalRate}%.1f/s"
      List(td(ingested), td(rate), td(maxUptimeLabel(g)))
    }

  /** The Ingested / Rate / Uptime cells for one member. */
  private def memberStatCells(info: V2IngestInfo): List[HtmlElement] =
    if (info.status == "Failed")
      List(errorCell(info.message.filter(_.nonEmpty).getOrElse("Unknown error")))
    else {
      val ingested = formatCount(info.stats.ingestedCount)
      val rate = f"${info.stats.rates.oneMinute}%.1f/s"
      val uptime = info.stats.totalRuntime.filter(_.nonEmpty).map(formatUptime).getOrElse("-")
      List(td(ingested), td(rate), td(uptime))
    }

  private def errorCell(message: String): HtmlElement =
    td(
      colSpan := 3,
      small(
        cls := "text-danger",
        i(cls := "cil-warning me-1"),
        message,
      ),
    )

  /** Combined failure message for a fully-failed group: the shared message if every
    * member reports the same one, else a count that points to the per-member breakdown.
    */
  private def failureSummary(g: IngestGroup): String =
    g.members.flatMap(_.message.filter(_.nonEmpty)).distinct match {
      case Nil => "Unknown error"
      case single :: Nil => single
      case _ => s"${g.size} members failed — expand for details"
    }

  private def maxUptimeLabel(g: IngestGroup): String =
    g.members
      .flatMap(_.stats.totalRuntime)
      .filter(_.nonEmpty)
      .maxByOption(uptimeSeconds)
      .map(formatUptime)
      .getOrElse("-")

  private def statusBadge(status: String): HtmlElement = {
    val badgeClass = status match {
      case "Running" => "badge bg-success"
      case "Paused" | "Restored" => "badge bg-warning text-dark"
      case "Failed" => "badge bg-danger"
      case Mixed => "badge bg-info text-dark"
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

  /** Total whole seconds represented by a duration string like "2h 3m 10s"; sub-second
    * segments are dropped. Returns 0 for an unparseable string.
    */
  private def uptimeSeconds(raw: String): Long = {
    var totalSeconds = 0L
    DurationSegment.findAllMatchIn(raw).foreach { m =>
      val value = m.group(1).toDouble
      m.group(2) match {
        case "h" => totalSeconds += (value * 3600).toLong
        case "m" => totalSeconds += (value * 60).toLong
        case "s" => totalSeconds += value.toLong
        case _ => // drop sub-second
      }
    }
    totalSeconds
  }

  private def formatUptime(raw: String): String = {
    val hasSegments = DurationSegment.findFirstIn(raw).isDefined
    if (!hasSegments) return raw

    val totalSeconds = uptimeSeconds(raw)
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

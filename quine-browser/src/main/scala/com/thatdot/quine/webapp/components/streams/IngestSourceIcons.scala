package com.thatdot.quine.webapp.components.streams

import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport

import com.raquo.laminar.api.L._

/** Icons for ingest source types.
  * Brand services use shared SVG assets from `public/shared-browser-resources/icons/`,
  * resolved via the `shared-resources` webpack alias. Other source types use CoreUI
  * free icons (cil-* classes).
  */
object IngestSourceIcons {

  // --- SVG file imports from shared-browser-resources (webpack alias: shared-resources) ---

  @JSImport("shared-resources/icons/kafka.svg", JSImport.Default)
  @js.native
  private object KafkaIcon extends js.Object

  @JSImport("shared-resources/icons/kinesis.svg", JSImport.Default)
  @js.native
  private object KinesisIcon extends js.Object

  @JSImport("shared-resources/icons/s3.svg", JSImport.Default)
  @js.native
  private object S3Icon extends js.Object

  @JSImport("shared-resources/icons/sqs.svg", JSImport.Default)
  @js.native
  private object SqsIcon extends js.Object

  /** Render a CoreUI icon element. */
  private def cilIcon(iconCls: String): HtmlElement =
    i(cls := s"$iconCls fs-4 me-3 flex-shrink-0")

  /** Render a brand SVG icon from an imported asset URL. */
  private def brandIcon(assetUrl: js.Object): HtmlElement =
    img(
      src := assetUrl.toString,
      cls := "me-3 flex-shrink-0 rounded",
      styleAttr := "width: 28px; height: 28px",
      alt := "",
    )

  /** Choose the icon for a given ingest source type.
    * Uses lowercase contains-matching so it works with both V1 slugs ("FileIngest")
    * and V2 class names ("File Ingest Stream").
    */
  def forSourceType(discValue: String, title: String): HtmlElement = {
    val key = (discValue + " " + title).toLowerCase
    if (key.contains("kafka")) brandIcon(KafkaIcon)
    else if (key.contains("kinesis")) brandIcon(KinesisIcon)
    else if (key.contains("s3") || key.contains("s3ingest")) brandIcon(S3Icon)
    else if (key.contains("sqs") || key.contains("queue")) brandIcon(SqsIcon)
    else if (key.contains("server sent") || key.contains("sse") || key.contains("eventsource")) cilIcon("cil-rss")
    else if (key.contains("websocket")) cilIcon("cil-transfer")
    else if (key.contains("file")) cilIcon("cil-file")
    else if (key.contains("stdin") || key.contains("standard input")) cilIcon("cil-terminal")
    else if (key.contains("number") || key.contains("iterator")) cilIcon("cil-list-numbered")
    else cilIcon("cil-stream")
  }
}

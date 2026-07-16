package com.thatdot.quine.webapp.components.landing

import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport

/** True-transparent SVG icons for service types used across the landing page.
  *
  * Each SVG lives in `src/main/resources/icons/`. Webpack (via scalajs-bundler) hashes the
  * URL at build time and `SvgRef.toString` returns the bundled URL at runtime.
  */
object ServiceIcons {

  @JSImport("shared-resources/icons/quine.png", JSImport.Default)
  @js.native
  private object QuineIcon extends js.Object

  @JSImport("shared-resources/icons/logo-icon.svg", JSImport.Default)
  @js.native
  private object QuineIconSvg extends js.Object

  /** URL for the white-on-transparent Quine "Q" icon (PNG). */
  def quineIcon: String = QuineIcon.toString

  /** URL for the blue gradient Quine "Q" icon (SVG). */
  def quineIconSvg: String = QuineIconSvg.toString

  @JSImport("shared-resources/icons/quine-wordmark.svg", JSImport.Default)
  @js.native
  private object QuineWordmarkSvg extends js.Object

  /** URL for the branded "Quine" wordmark (blue gradient SVG text). */
  def quineWordmark: String = QuineWordmarkSvg.toString

  @JSImport("shared-resources/icons/kafka.svg", JSImport.Default)
  @js.native
  private object KafkaSvg extends js.Object

  @JSImport("shared-resources/icons/s3.svg", JSImport.Default)
  @js.native
  private object S3Svg extends js.Object

  @JSImport("shared-resources/icons/kinesis.svg", JSImport.Default)
  @js.native
  private object KinesisSvg extends js.Object

  @JSImport("shared-resources/icons/slack.svg", JSImport.Default)
  @js.native
  private object SlackSvg extends js.Object

  @JSImport("shared-resources/icons/http.svg", JSImport.Default)
  @js.native
  private object HttpSvg extends js.Object

  @JSImport("shared-resources/icons/sqs.svg", JSImport.Default)
  @js.native
  private object SqsSvg extends js.Object

  @JSImport("shared-resources/icons/stdout.svg", JSImport.Default)
  @js.native
  private object StdoutSvg extends js.Object

  @JSImport("shared-resources/icons/cassandra.svg", JSImport.Default)
  @js.native
  private object CassandraSvg extends js.Object

  @JSImport("shared-resources/icons/rocksdb.svg", JSImport.Default)
  @js.native
  private object RocksdbSvg extends js.Object

  @JSImport("shared-resources/icons/clickhouse.svg", JSImport.Default)
  @js.native
  private object ClickhouseSvg extends js.Object

  @JSImport("shared-resources/icons/file.svg", JSImport.Default)
  @js.native
  private object FileSvg extends js.Object

  @JSImport("shared-resources/icons/drop.svg", JSImport.Default)
  @js.native
  private object DropSvg extends js.Object

  @JSImport("shared-resources/icons/number-iterator.png", JSImport.Default)
  @js.native
  private object NumberIteratorPng extends js.Object

  /** Get icon URL for a given source/output/persistor type.
    * Matches all known type names: API class names (e.g. "StandardOut"), config keys (e.g. "rocks-db"),
    * and source class names (e.g. "NumberIteratorIngest"). Case-insensitive, strips hyphens/underscores.
    */
  def forType(nodeType: String): Option[String] = nodeType.toLowerCase.replace("-", "").replace("_", "") match {
    case "kafka" => Some(KafkaSvg.toString)
    case "s3" => Some(S3Svg.toString)
    case "file" => Some(FileSvg.toString)
    case "kinesis" | "kinesiskcl" => Some(KinesisSvg.toString)
    case "sqs" => Some(SqsSvg.toString)
    case "slack" => Some(SlackSvg.toString)
    case "http" | "httpendpoint" | "webhook" => Some(HttpSvg.toString)
    case "drop" => Some(DropSvg.toString)
    case "standardout" | "stdout" | "console" => Some(StdoutSvg.toString)
    case "cassandra" | "keyspaces" => Some(CassandraSvg.toString)
    case "rocksdb" => Some(RocksdbSvg.toString)
    case "clickhouse" => Some(ClickhouseSvg.toString)
    case "numberiterator" | "numberiteratoringest" => Some(NumberIteratorPng.toString)
    case "serversentevent" | "serversenteventingest" | "sse" => Some(HttpSvg.toString)
    case _ => None
  }

  /** Prettified display label for a source/output type slug. */
  def labelFor(nodeType: String): String = nodeType.toLowerCase match {
    case "kafka" => "Kafka"
    case "s3" => "S3"
    case "file" => "File"
    case "kinesis" => "Kinesis"
    case "kinesiskcl" => "Kinesis (KCL)"
    case "slack" => "Slack"
    case "http" | "httpendpoint" => "HTTP"
    case "stdout" | "standardout" | "console" => "StdOut"
    case "numberiterator" => "Number Iterator"
    case "reactivestream" => "Reactive Stream"
    case "sqs" => "SQS"
    case "sns" => "SNS"
    case "sse" | "serversentevent" | "serversentevents" => "SSE"
    case "websocketclient" | "websocketsimplestartupingest" => "WebSocket"
    case "stdinput" | "standardinput" | "stdin" => "StdIn"
    case "cypher" | "cypherquery" => "Cypher"
    case "drop" => "Drop"
    case other => other.capitalize
  }
}

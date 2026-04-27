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

  /** URL for the white-on-transparent Quine "Q" icon. */
  def quineIcon: String = QuineIcon.toString

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

  @JSImport("shared-resources/icons/number-iterator.png", JSImport.Default)
  @js.native
  private object NumberIteratorPng extends js.Object

  /** Get icon URL for a given source/output/persistor type. */
  def forType(nodeType: String): Option[String] = nodeType.toLowerCase match {
    case "kafka" => Some(KafkaSvg.toString)
    case "s3" | "file" => Some(S3Svg.toString)
    case "kinesis" | "kinesiskcl" => Some(KinesisSvg.toString)
    case "slack" => Some(SlackSvg.toString)
    case "http" | "httpendpoint" => Some(HttpSvg.toString)
    case "stdout" | "standardout" | "console" => Some(StdoutSvg.toString)
    case "cassandra" | "keyspaces" => Some(CassandraSvg.toString)
    case "rocksdb" => Some(RocksdbSvg.toString)
    case "clickhouse" => Some(ClickhouseSvg.toString)
    case "numberiterator" => Some(NumberIteratorPng.toString)
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

package com.thatdot.quine.app.model.outputs2.definitions.destination

import java.nio.charset.StandardCharsets

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Sink

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.model.outputs2.definitions.ResultDestination
import com.thatdot.quine.graph.NamespaceId

final case class StandardOut(
  logLevel: StandardOut.LogLevel = StandardOut.LogLevel.Info,
  logMode: StandardOut.LogMode = StandardOut.LogMode.Complete,
) extends ResultDestination.Bytes.StandardOut {
  override def sink(name: String, inNamespace: NamespaceId)(implicit logConfig: LogConfig): Sink[Array[Byte], NotUsed] =
    Sink
      .foreach[Array[Byte]] { bytes =>
        System.out.write(bytes)
        System.out.write("\n".getBytes(StandardCharsets.UTF_8))
      }
      .mapMaterializedValue(_ => NotUsed)
}

object StandardOut {

  sealed abstract class LogMode

  object LogMode {
    case object Complete extends LogMode
    case object FastSampling extends LogMode

    val modes: Seq[LogMode] = Vector(Complete, FastSampling)
  }

  sealed abstract class LogLevel
  object LogLevel {
    case object Trace extends LogLevel
    case object Debug extends LogLevel
    case object Info extends LogLevel
    case object Warn extends LogLevel
    case object Error extends LogLevel

    val levels: Seq[LogLevel] = Vector(Trace, Debug, Info, Warn, Error)
  }
}

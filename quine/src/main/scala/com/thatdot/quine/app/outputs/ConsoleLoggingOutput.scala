package com.thatdot.quine.app.outputs

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import com.thatdot.common.logging.Log.{
  LazySafeLogging,
  LogConfig,
  Safe,
  SafeInterpolator,
  SafeLoggableInterpolator,
  SafeLogger,
}
import com.thatdot.quine.app.outputs.ConsoleLoggingOutput.{printLogger, printLoggerNonBlocking}
import com.thatdot.quine.graph.{CypherOpsGraph, MasterStream, NamespaceId, StandingQueryResult}
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.PrintToStandardOut
class ConsoleLoggingOutput(val config: PrintToStandardOut)(implicit
  private val logConfig: LogConfig,
) extends OutputRuntime
    with LazySafeLogging {

  def flow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  ): Flow[StandingQueryResult, MasterStream.SqResultsExecToken, NotUsed] = {
    val token = execToken(name, inNamespace)
    val PrintToStandardOut(logLevel, logMode, structure) = config

    import PrintToStandardOut._
    val resultLogger: SafeLogger = logMode match {
      case LogMode.Complete => printLogger
      case LogMode.FastSampling => printLoggerNonBlocking
    }
    val logFn: SafeInterpolator => Unit =
      logLevel match {
        case LogLevel.Trace => resultLogger.trace(_)
        case LogLevel.Debug => resultLogger.debug(_)
        case LogLevel.Info => resultLogger.info(_)
        case LogLevel.Warn => resultLogger.warn(_)
        case LogLevel.Error => resultLogger.error(_)
      }

    Flow[StandingQueryResult].map { result =>
      // NB we are using `Safe` here despite `result` potentially containing PII because the entire purpose of this
      // output is to log SQ results. If the user has configured this output, they have accepted the risk of PII
      // in their logs.
      logFn(
        log"Standing query `${Safe(name)}` match: ${Safe(result.toJson(structure)(graph.idProvider, logConfig).noSpaces)}",
      )
      token
    }
  }
}
object ConsoleLoggingOutput {
  // Invariant: these keys must be fixed to the names of the loggers in Quine App's application.conf
  private val printLogger = SafeLogger("thatdot.StandingQueryResults")
  private val printLoggerNonBlocking = SafeLogger("thatdot.StandingQueryResultsSampled")
}

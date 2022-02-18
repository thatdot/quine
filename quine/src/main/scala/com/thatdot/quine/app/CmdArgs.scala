package com.thatdot.quine.app

import scopt.OEffect._
import scopt.OParser

/** Data model for Quine command line program arguments.
  *
  * @param disableWebservice indicates if the web service should not be started
  * @param port indicates what port the web service should be started on
  * @param recipe specifies a recipe (by URL or file path) to be loaded and executed
  * @param recipeValues specifies recipe parameter substitution values
  * @param printVersion indicates the program should print the current version and exit
  * @param forceConfig indicates the typelevel configuration should be read and used verbatim,
  *                    without overrides that normally occur to persistence configuration
  * @param deleteDataFile indicates the local database file should not be deleted when the program exists
  */
final case class CmdArgs(
  disableWebservice: Boolean = false,
  port: Option[Int] = None,
  recipe: Option[String] = None,
  recipeValues: Map[String, String] = Map.empty[String, String],
  printVersion: Boolean = false,
  forceConfig: Boolean = false,
  deleteDataFile: Boolean = true
)

object CmdArgs {

  /** Uses scopt library to parse command line arguments to the CmdArgs data model. */
  def apply(args: Array[String]): Either[String, CmdArgs] = {
    val builder = OParser.builder[CmdArgs]
    val parser = {
      import builder._
      OParser.sequence(
        programName("quine"),
        head("Quine universal program"),
        opt[Unit]('W', "disable-web-service")
          .action((_, c) => c.copy(disableWebservice = true))
          .text("disable Quine web service"),
        opt[Int]('p', "port")
          .action((port, c) => c.copy(port = Some(port)))
          .text("web service port (default is 8080)"),
        opt[String]('r', "recipe")
          .action((url, c) => c.copy(recipe = Some(url)))
          .valueName("name, file, or URL")
          .text("follow the specified recipe"),
        opt[Map[String, String]]('x', "recipe-value")
          .unbounded()
          .action((x, c) => c.copy(recipeValues = c.recipeValues ++ x))
          .text("recipe parameter substitution")
          .valueName("key=value"),
        opt[Unit]("force-config")
          .action((_, c) => c.copy(forceConfig = true))
          .text("disable recipe configuration defaults"),
        opt[Unit]("no-delete")
          .action((_, c) => c.copy(deleteDataFile = false))
          .text("disable deleting data file when process exits"),
        help('h', "help"),
        opt[Unit]('v', "version")
          .action((_, c) => c.copy(printVersion = true))
          .text("print Quine program version"),
        checkConfig { c =>
          if (c.forceConfig && !c.deleteDataFile) {
            failure("use only one: --force-config, or --no-delete")
          } else if (c.disableWebservice && c.port.isDefined) {
            failure("use only one: --disable-web-service, or --port")
          } else {
            Right(())
          }
        }
      )
    }

    OParser.runParser(parser, args, CmdArgs()) match {
      case (_, effects) if effects.nonEmpty =>
        Left {
          effects collect {
            case DisplayToOut(msg: String) => msg
            case DisplayToErr(msg: String) => msg
            case ReportError(msg: String) => s"Warning: $msg"
            case ReportWarning(msg: String) => s"Error: $msg"
          } mkString "\n"
        }
      case (Some(config), _) => Right(config)
      case _ => Left("Error: unknown") // TODO
    }
  }
}

package com.thatdot.quine.app

import java.io.PrintStream

import scala.collection.mutable
import scala.concurrent.blocking

import com.typesafe.scalalogging.Logger

class StatusLines(
  logger: Logger,
  realtimeOutput: PrintStream
) {

  /** Logs an informational message and refreshes the status lines display.
    * @param message
    */
  def info(message: String): Unit = {
    logger.info(message)
    refreshStatusLines()
  }

  /** Logs an warning message and refreshes the status lines display.
    * @param message
    */
  def warn(message: String): Unit = {
    logger.warn(message)
    refreshStatusLines()
  }

  /** Logs an warning message and refreshes the status lines display.
    * @param message
    */
  def warn(message: String, t: Throwable): Unit = {
    logger.warn(message, t)
    refreshStatusLines()
  }

  /** Logs an error message and refreshes the status lines display.
    * @param message
    */
  def error(message: String): Unit = {
    logger.error(message)
    refreshStatusLines()
  }

  /** Logs an error message and refreshes the status lines display.
    * @param message
    */
  def error(message: String, t: Throwable): Unit = {
    logger.error(message, t)
    refreshStatusLines()
  }

  class StatusLine

  // Using LinkedHashMap so that status messages will be printed in insertion order
  private val messages: mutable.LinkedHashMap[StatusLine, String] = mutable.LinkedHashMap.empty[StatusLine, String]

  def create(message: String = ""): StatusLine = {
    val statusLine = new StatusLine
    blocking(messages.synchronized {
      messages += statusLine -> message
    })
    refreshStatusLines()
    statusLine
  }

  def update(statusLine: StatusLine, message: String): Unit = {
    blocking(messages.synchronized {
      messages += statusLine -> message
    })
    refreshStatusLines()
  }

  def remove(statusLine: StatusLine): Unit = {
    blocking(messages.synchronized {
      messages -= statusLine
    })
    refreshStatusLines(clearExtraLine = true)
  }

  /** Prints status lines as follows: an empty line, then the status lines, then
    * the cursor is moved to the leftmost column of the blank line.
    *
    * @param clearExtraLine set to true after removing a status line, to account for
    *                       the line that needs to be cleared
    */
  private def refreshStatusLines(clearExtraLine: Boolean = false): Unit = this.synchronized {
    val up1 = "\u001b[1A"
    val erase = "\u001b[K"
    val home = "\r"
    val homeErase = home + erase
    realtimeOutput.println(homeErase)
    val statuses = messages.values.toSeq.filter(_.trim != "")
    for { status <- statuses } realtimeOutput.println(s"$homeErase | => $status")
    if (clearExtraLine) realtimeOutput.print(homeErase)
    for { _ <- 1 to statuses.length + 1 } realtimeOutput.print(up1)
  }
}

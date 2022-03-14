package com.thatdot.quine.app

import java.io.PrintStream

import scala.collection.mutable

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
    messages.synchronized {
      messages += statusLine -> message
    }
    refreshStatusLines()
    statusLine
  }

  def update(statusLine: StatusLine, message: String): Unit = {
    messages.synchronized {
      messages += statusLine -> message
    }
    refreshStatusLines()
  }

  def remove(statusLine: StatusLine): Unit = {
    messages.synchronized {
      messages -= statusLine
    }
    refreshStatusLines(extraSpace = true)
  }

  /** Prints status lines as follows: an empty line, then the status lines, then
    * the cursor is moved to the leftmost column of the blank line.
    */
  private def refreshStatusLines(extraSpace: Boolean = false): Unit = {
    val up1 = "\u001b[1A"
    val erase = "\u001b[K"
    val home = "\r"
    realtimeOutput.println(s"$home$erase")
    val stati = messages.values.toSeq.filter(_.trim != "")
    for { status <- stati } realtimeOutput.println(s"$home$erase | => $status")
    if (extraSpace) realtimeOutput.println(s"$home$erase")
    for { _ <- 1 to stati.length + 1 } realtimeOutput.print(up1)
    if (extraSpace) realtimeOutput.print(up1)
  }
}

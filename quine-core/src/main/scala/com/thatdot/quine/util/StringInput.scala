package com.thatdot.quine.util

import java.io.File
import java.net.{MalformedURLException, URL}

object StringInput {

  /** Allow a URL or a local filename to be passed as a string.
    * Falls back to treating the string as a local filename if it doesn't parse
    * as a valid URL
    */
  def filenameOrUrl(s: String): URL = try new URL(s)
  catch {
    case _: MalformedURLException =>
      // Handle the case where just a filename is given by converting the file path
      // to a URL
      new File(s).toURI.toURL
  }

}

package com.thatdot.quine.exceptions

import com.thatdot.quine.util.QuineError

/** Raised when an ingest source is configured with a format it cannot support — e.g. a
  * random-access format (Parquet) requested over a non-seekable input (stdin, websocket).
  */
case class IngestSourceFormatException(msg: String) extends QuineError {
  override def getMessage: String = msg
}

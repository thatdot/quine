package com.thatdot.quine.exceptions

import com.thatdot.quine.util.QuineError

case class DuplicateIngestException(ingestName: String, namespace: Option[String]) extends QuineError {
  override def getMessage: String = namespace match {
    case None => s"Ingest $ingestName already exists"
    case Some(ns) => s"Ingest $ingestName already exists in namespace namespace $ns"
  }
}

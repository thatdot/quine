package com.thatdot.quine.exceptions

import com.thatdot.quine.util.QuineError

/** A member-local ingest could not be created because its name is already in use by a running
  * cluster-ingest partition worker on this member. A partition worker's name is derived from its
  * cluster ingest's (`name#index`), and it shares a member ingest's counting surfaces, so the two
  * may not carry the same name at once.
  */
case class ReservedIngestNameException(ingestName: String, namespace: String) extends QuineError {
  override def getMessage: String =
    s"Ingest '$ingestName' cannot be created in namespace '$namespace': the name is in use by a " +
    "running cluster-ingest partition worker on this member."
}

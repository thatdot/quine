package com.thatdot.quine.model

import com.thatdot.quine.model.QuineId

object QuineIdHelpers {
  implicit class QuineIdOps(quineId: QuineId) {
    def pretty(implicit idProvider: QuineIdProvider): String =
      idProvider.qidToPrettyString(quineId)
  }
}

package com.thatdot.api.v2

trait ShowShort[-A] {
  def showShort(a: A): String
}

trait ShowShortOps {
  implicit class ShortShower[A: ShowShort](a: A) {
    def showShort: String = ShowShort[A].showShort(a)
  }
}

object ShowShort {
  def apply[A](implicit instance: ShowShort[A]): ShowShort[A] = instance

  implicit def eitherShowShort[A: ShowShort, B: ShowShort]: ShowShort[Either[A, B]] =
    (eitherAB: Either[A, B]) => eitherAB.fold(ShowShort[A].showShort, ShowShort[B].showShort)

  implicit def hasErrorShowShort[A <: HasError]: ShowShort[A] = (hasError: A) => {
    // Structured details (RequestInfo, ErrorInfo) carry short log-safe identifiers — render inline
    // so an operator looking at audit logs can lift the correlation UUID or the machine-readable
    // reason directly. Help is unbounded prose, so summarize as a count to avoid spamming log lines.
    val inline = hasError.details
      .collect {
        case ErrorDetail.RequestInfo(requestId) => s"requestId=$requestId"
        case ErrorDetail.ErrorInfo(reason, _, _) => s"reason=$reason"
      }
      .mkString(", ")
    val helpCount = hasError.details.count(_.isInstanceOf[ErrorDetail.Help])
    val inlinePart = if (inline.isEmpty) "" else s"; $inline"
    val helpPart = if (helpCount == 0) "" else s" (+$helpCount help)"
    s"[${hasError.message}$inlinePart$helpPart]"
  }

  object syntax extends ShowShortOps
}

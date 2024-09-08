package com.thatdot.quine.routes.exts

import endpoints4s.algebra.Endpoints

/** Override the error text defined in [[endpoints4s.algebra.Errors]]
  * with more informative values.
  */
trait EndpointsWithCustomErrorText extends Endpoints {

  // note that the mailto: link for support is omitted here because
  // stoplight does not correctly render mailto links.

  private val badRequestDoc =
    """Bad Request

  Something in your request is invalid, and Quine could not process it.
  Review your request and attempt to submit it again.

  %s

  Contact support if you continue to have issues.
  """.stripMargin

  private val serverErrorDoc =
    """Internal Server Error
          |
          |Quine encountered an unexpected condition that prevented processing your request.
          |
          |  %s
          |
          |  Contact support if you continue to have issues.""".stripMargin

  /** Manually generate a markdown bullet list from the list of message strings. */
  private def buildErrorMessage(docs: String, messages: Seq[String]): String =
    if (messages.isEmpty) ""
    else {
      val bulletSeparator = "\n - "
      val msgString = f"Possible errors:$bulletSeparator${messages.mkString(bulletSeparator)}"
      docs.format(msgString)
    }

  override lazy val clientErrorsResponse: Response[ClientErrors] =
    badRequest(docs = Some(f"${badRequestDoc.format("")}"))

  override lazy val serverErrorResponse: Response[ServerError] =
    internalServerError(docs = Some(f"${serverErrorDoc.format("")}"))

  def customBadRequest(messages: String*): Response[ClientErrors] = badRequest(
    Some(buildErrorMessage(badRequestDoc, messages)),
  )
  def customServerError(messages: String*): Response[ServerError] = internalServerError(
    Some(buildErrorMessage(serverErrorDoc, messages)),
  )

}

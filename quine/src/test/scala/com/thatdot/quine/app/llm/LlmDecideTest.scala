package com.thatdot.quine.app.llm

import java.net.ConnectException

import scala.concurrent.Future

import org.apache.pekko.stream.scaladsl.Sink

import com.thatdot.quine.app.config.{DefaultProfiles, LlmApi, LlmProfile}
import com.thatdot.quine.compiler.cypher.{CypherHarness, queryCypherValues, registerUserDefinedProcedure}
import com.thatdot.quine.exceptions.ExternalServiceException

/** `llm.decide` end to end through the Cypher interpreter, against a scripted decision client. */
class LlmDecideTest extends CypherHarness("llm-decide") {

  private val unreachable = DecisionClient(
    "jev-down",
    LlmProfile.Decision(LlmApi.JevSystemOne, "http://jev-down.test", "j"),
    (_, _) =>
      Future.failed(
        new LlmError(
          "could not reach the model endpoint (ConnectException)",
          LlmError.Kind.Retryable,
          LlmError.Fault.Unavailable,
        )(Some(new ConnectException("Connection refused: jev-down.test/10.0.0.1:80"))),
      ),
  )
  registerUserDefinedProcedure(
    new LlmDecide(LlmClients(Map("jev-down" -> unreachable), DefaultProfiles(decision = List("jev-down")))),
  )

  // See the matching test in LlmPromptTest: an escaped LlmError would fail recoverToExceptionIf,
  // and the cause on the error holds the host, which may not leave the package
  describe("no LlmError leaves the procedure, and neither does its cause") {
    it("a transport failure surfaces as an ExternalServiceException with no cause attached") {
      recoverToExceptionIf[ExternalServiceException](
        queryCypherValues(
          """CALL llm.decide({text: "x"}, {q: {type: "yes-no", instructions: "?"}}) YIELD answers RETURN answers""",
          cypherHarnessNamespace,
        )(graph).results.runWith(Sink.ignore),
      ).map { e =>
        assert(e.getMessage == "llm.decide: could not reach the model endpoint (ConnectException)")
        assert(e.failure == ExternalServiceException.Failure.Unavailable)
        assert(e.getCause == null, "the cause may hold the host, so it stays inside the package")
      }
    }
  }
}

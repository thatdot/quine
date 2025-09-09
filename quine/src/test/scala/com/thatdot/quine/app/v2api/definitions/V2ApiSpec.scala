package com.thatdot.quine.app.v2api.definitions

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import sttp.capabilities
import sttp.client3.SttpBackend
import sttp.client3.testing.SttpBackendStub
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.server.stub.TapirStubInterpreter

import com.thatdot.api.v2.SuccessEnvelope

/** Support for providing Endpoint stubs for testing. */
trait V2ApiSpec {

  def endpointStub[INPUT, OUTPUT](
    endpoint: Full[_, _, INPUT, Any, SuccessEnvelope[OUTPUT], Any, Future],
  ): SttpBackend[Future, capabilities.WebSockets] =
    TapirStubInterpreter(SttpBackendStub.asynchronousFuture)
      .whenServerEndpointRunLogic(endpoint)
      .backend()

  implicit class Unwrapper[T](t: Future[T]) {
    def unwrap: T = Await.result(t, Duration.Inf)
  }

  /* example test
  val backendStub = endpointStub(propertiesEndpoint)

  // when
  val response: Future[Response[Either[String, String]]] = basicRequest
    .get(uri"http://any:8081/api/v2/admin")
    .contentType("application/json")
    .send(backendStub)

   */
}

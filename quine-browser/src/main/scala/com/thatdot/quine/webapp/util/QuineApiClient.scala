package com.thatdot.quine.webapp.util

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.scalajs.dom
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.AuthEvents
import com.thatdot.quine.webapp.v2api.V2ApiTypes._

/** Lightweight V2 API fetch-and-poll utilities shared across components.
  *
  * Provides a namespace-aware `standingQueries` feed and the generic `fetchV2`/`poll`
  * primitives that back it. Components consume a [[QuineApiClient.Feed]] without
  * knowing where or how often data is fetched.
  */
object QuineApiClient {

  val PollIntervalMs: Int = 5000

  /** A polled feed: successful values and failure messages on separate streams. */
  final case class Feed[A](values: EventStream[A], errors: EventStream[String])

  /** Poll `fetch` on a fixed interval, splitting results into success/error streams. */
  def poll[A](fetch: => Future[A]): Feed[A] = {
    val ticks = PollingStream(PollIntervalMs) {
      fetch.transform(scala.util.Success(_))
    }
    Feed(
      values = ticks.collect { case scala.util.Success(value) => value },
      errors = ticks.collect { case scala.util.Failure(t) => errorMessage(t) },
    )
  }

  /** Common transport for V2 API calls: resolves base URL, maps 401 onto the auth bus,
    * and fails any non-2xx. The successful response is handed back unread.
    */
  private def requestV2(path: String, routes: ClientRoutes, init: dom.RequestInit): Future[dom.Response] = {
    val url = routes.baseUrlOpt match {
      case Some(base) => s"${base.stripSuffix("/")}/$path"
      case None => path
    }
    dom
      .fetch(url, init)
      .toFuture
      .recoverWith { case _ =>
        Future.failed(new RuntimeException("Cannot reach server"))
      }
      .flatMap { response =>
        if (response.status == 401) {
          AuthEvents.unauthorized.emit(())
          Future.failed(new RuntimeException(s"Server returned ${response.status} ${response.statusText}"))
        } else if (!response.ok) {
          Future.failed(new RuntimeException(s"Server returned ${response.status} ${response.statusText}"))
        } else Future.successful(response)
      }
  }

  /** Fetch and decode a V2 API response, respecting the base URL from [[ClientRoutes]]. */
  def fetchV2[A: Decoder](path: String, routes: ClientRoutes): Future[A] = {
    val init = new dom.RequestInit {
      this.method = dom.HttpMethod.GET
    }
    requestV2(path, routes, init).flatMap { response =>
      response.text().toFuture.flatMap { body =>
        io.circe.parser.decode[A](body) match {
          case Right(value) => Future.successful(value)
          case Left(_) => Future.failed(new RuntimeException("Unexpected response from server"))
        }
      }
    }
  }

  /** PUT a JSON body to a V2 API endpoint. Completes with Unit on any 2xx response
    * (the V2 PUT endpoints we target return 204 No Content).
    */
  def putV2[A: Encoder](path: String, payload: A, routes: ClientRoutes): Future[Unit] = {
    val reqHeaders = new dom.Headers()
    reqHeaders.set("Content-Type", "application/json")
    reqHeaders.set("Accept", "application/json")
    val encoded = payload.asJson.noSpaces
    val init = new dom.RequestInit {
      this.method = dom.HttpMethod.PUT
      this.headers = reqHeaders
      this.body = encoded
    }
    requestV2(path, routes, init).map(_ => ())
  }

  /** Polled feed of standing queries for the given graph namespace. */
  def standingQueries(graphName: String, routes: ClientRoutes): Feed[Seq[V2StandingQueryInfo]] =
    poll(fetchV2[V2Page[V2StandingQueryInfo]](s"api/v2/graph/$graphName/standingQueries", routes).map(_.items))

  private def errorMessage(t: Throwable): String = {
    val raw = Option(t.getMessage).filter(_.nonEmpty)
    raw match {
      case None => "Cannot reach server"
      case Some(m) if m.contains("Failed to fetch") || m.contains("NetworkError") || m.contains("Load failed") =>
        "Cannot reach server"
      case Some(m) => m
    }
  }
}

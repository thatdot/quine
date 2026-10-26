package com.thatdot.quine.app.llm

import org.apache.pekko.actor.ActorSystem

import cats.syntax.traverse._
import com.codahale.metrics.MetricRegistry

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.config.{DefaultProfiles, LlmApi, LlmConfig, LlmProfile}

/** A built client: a profile's name, the profile, and its client function. Mirrors [[LlmProfile]]: one sealed
  * trait, one variant per side, held in one map.
  */
sealed trait LlmClient {
  def name: String
  def profile: LlmProfile
}

final case class CompletionClient(name: String, profile: LlmProfile.Completion, complete: Completion.Complete)
    extends LlmClient
final case class DecisionClient(name: String, profile: LlmProfile.Decision, decide: Decision.Decide) extends LlmClient

/** The configured profiles' clients by name. Every name in a call's chain must resolve before anything is sent:
  * a typo in a fallback is a bug to see, not a failing profile to skip.
  */
final case class LlmClients(
  clients: Map[String, LlmClient],
  defaults: DefaultProfiles,
) {

  private def resolveCompletion(name: String): Either[String, CompletionClient] =
    clients.get(name) match {
      case Some(client: CompletionClient) => Right(client)
      case Some(other) =>
        Left(
          s"""profile "$name" has kind ${other.profile.api.kindName}, """ +
          s"this procedure needs a ${LlmApi.completionKindName} profile",
        )
      case None => Left(s"""no LLM profile named "$name" is configured under llm.profiles""")
    }

  private def resolveDecision(name: String): Either[String, DecisionClient] =
    clients.get(name) match {
      case Some(client: DecisionClient) => Right(client)
      case Some(other) =>
        Left(
          s"""profile "$name" has kind ${other.profile.api.kindName}, """ +
          s"this procedure needs a ${LlmApi.decisionKindName} profile",
        )
      case None => Left(s"""no LLM profile named "$name" is configured under llm.profiles""")
    }

  /** The clients the call tries in order: the names it passed, or the default chain. Never `Right(Nil)`:
    * the options reader rejects an empty list and empty defaults land in the no-default arm.
    */
  def completionChain(requested: Option[List[String]]): Either[String, List[CompletionClient]] =
    requested.getOrElse(defaults.completion) match {
      case Nil if !clients.values.exists(_.isInstanceOf[CompletionClient]) =>
        Left(s"no ${LlmApi.completionKindName} LLM profile is configured under llm.profiles")
      case Nil =>
        Left(
          s"no default ${LlmApi.completionKindName} profile is configured under llm.default-profiles " +
          "and the call named no profile",
        )
      case names => names.traverse(resolveCompletion)
    }

  def decisionChain(requested: Option[List[String]]): Either[String, List[DecisionClient]] =
    requested.getOrElse(defaults.decision) match {
      case Nil if !clients.values.exists(_.isInstanceOf[DecisionClient]) =>
        Left(s"no ${LlmApi.decisionKindName} LLM profile is configured under llm.profiles")
      case Nil =>
        Left(
          s"no default ${LlmApi.decisionKindName} profile is configured under llm.default-profiles " +
          "and the call named no profile",
        )
      case names => names.traverse(resolveDecision)
    }
}

object LlmClients {

  val empty: LlmClients = LlmClients(Map.empty, DefaultProfiles())

  def fromConfig(config: LlmConfig, metrics: MetricRegistry)(implicit
    system: ActorSystem,
    logConfig: LogConfig,
  ): LlmClients = fromConfig(config, metrics, LlmHttp.pekkoPool)

  /** One client per profile, each on its own [[PostToLlmProfile]]. The defaults were checked against the profiles
    * when the config was read, so a bad default never reaches here.
    */
  def fromConfig(config: LlmConfig, metrics: MetricRegistry, poolFor: LlmHttp.PoolFor)(implicit
    system: ActorSystem,
    logConfig: LogConfig,
  ): LlmClients = {
    val clients: Map[String, LlmClient] = config.profiles.map {
      case (name, p: LlmProfile.Completion) =>
        name -> (CompletionClient(
          name,
          p,
          Completion.complete(p, new PostToLlmProfile(name, p, poolFor, metrics)),
        ): LlmClient)
      case (name, p: LlmProfile.Decision) =>
        // One decision api today. A second one adds an arm here and a `decide` in its own object.
        val decide: Decision.Decide = p.api match {
          case LlmApi.JevSystemOne => Jev.decide(p, new PostToLlmProfile(name, p, poolFor, metrics))
        }
        name -> (DecisionClient(name, p, decide): LlmClient)
    }
    LlmClients(clients, config.defaultProfiles)
  }
}

package com.thatdot.quine.app.v2api.definitions

import java.util.Properties

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.apache.pekko.util.Timeout

import cats.data.NonEmptyList
import cats.implicits.toFunctorOps

import com.thatdot.quine.app.config.BaseConfig
import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator.ErrorString
import com.thatdot.quine.app.routes.{GenerateMetrics, StandingQueryStore}
import com.thatdot.quine.app.v2api.endpoints.V2AdministrationEndpointEntities.{TGraphHashCode, TQuineInfo}
import com.thatdot.quine.app.{BaseApp, BuildInfo, NamespaceNotFoundException}
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.{
  BaseGraph,
  InMemoryNodeLimit,
  InvalidQueryPattern,
  LiteralOpsGraph,
  NamespaceId,
  StandingQueryOpsGraph,
  namespaceToString
}
import com.thatdot.quine.model.{Milliseconds, QuineId}
import com.thatdot.quine.persistor.PersistenceAgent
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.WriteToKafka
import com.thatdot.quine.routes._
import com.thatdot.quine.{BuildInfo => QuineBuildInfo}

/** Access to application components for api endpoints */
trait ApplicationApiInterface {
  val graph: BaseGraph with LiteralOpsGraph with StandingQueryOpsGraph

  implicit def timeout: Timeout

  val app: BaseApp with StandingQueryStore

  val config: BaseConfig

  def thisMemberIdx: Int

  //  endpoint business logic functionality.
  def getProperties: Future[Map[String, String]] = {
    val props: Properties = System.getProperties
    Future.successful(props.keySet.asScala.map(s => s.toString -> props.get(s).toString).toMap[String, String])
  }

  def getNamespaces: Future[List[String]] = Future.apply {
    graph.requiredGraphIsReady()
    app.getNamespaces.map(namespaceToString).toList
  }(ExecutionContext.parasitic)

  def createNamespace(namespace: String): Future[Boolean] =
    app.createNamespace(Some(Symbol(namespace)))

  def deleteNamespace(namespace: String): Future[Boolean] =
    app.deleteNamespace(Some(Symbol(namespace)))

  def listAllStandingQueries: Future[List[RegisteredStandingQuery]] = {
    implicit val executor = ExecutionContext.parasitic
    Future.sequence(app.getNamespaces.map(app.getStandingQueries)).map(_.toList.flatten)
  }

  // --------------------- Admin Endpoints ------------------------
  def graphHashCode(atTime: Option[Milliseconds], namespace: NamespaceId): Future[TGraphHashCode] =
    graph.requiredGraphIsReadyFuture {
      val at = atTime.getOrElse(Milliseconds.currentTime())
      graph
        .getGraphHashCode(namespace, Some(at))
        .map(TGraphHashCode(_, at.millis))(ExecutionContext.parasitic)
    }

  def buildInfo: TQuineInfo = {
    val gitCommit: Option[String] = QuineBuildInfo.gitHeadCommit
      .map(_ + (if (QuineBuildInfo.gitUncommittedChanges) "-DIRTY" else ""))
    TQuineInfo(
      BuildInfo.version,
      gitCommit,
      QuineBuildInfo.gitHeadCommitDate,
      QuineBuildInfo.javaVmName + " " + QuineBuildInfo.javaVersion + " (" + QuineBuildInfo.javaVendor + ")",
      PersistenceAgent.CurrentVersion.shortString
    )
  }

  def emptyConfigExample: BaseConfig

  def isReady = graph.isReady
  def isLive = true

  def performShutdown(): Future[Unit] = graph.system.terminate().map(_ => ())(ExecutionContext.parasitic)

  def metaData: Future[Map[String, String]] = {
    implicit val ec = ExecutionContext.parasitic
    graph.namespacePersistor.getAllMetaData().flatMap { m =>
      Future.successful(m.view.mapValues(new String(_)).toMap)
    }
  }

  def metrics: MetricsReport = GenerateMetrics.metricsReport(graph)

  def shardSizes(resizes: Map[Int, ShardInMemoryLimit]): Future[Map[Int, ShardInMemoryLimit]] =
    graph
      .shardInMemoryLimits(resizes.fmap(l => InMemoryNodeLimit(l.softLimit, l.hardLimit)))
      .map(_.collect { case (shardIdx, Some(InMemoryNodeLimit(soft, hard))) =>
        shardIdx -> ShardInMemoryLimit(soft, hard)
      })(ExecutionContext.parasitic)

  def requestNodeSleep(quineId: QuineId, namespaceId: NamespaceId): Future[Unit] =
    graph.requiredGraphIsReadyFuture(
      graph.requestNodeSleep(namespaceId, quineId)
    )

  // --------------------- Standing Query Endpoints ------------------------

  def listStandingQueries(namespaceId: NamespaceId): Future[List[RegisteredStandingQuery]] =
    graph.requiredGraphIsReadyFuture {
      app.getStandingQueries(namespaceId)
    }

  def propagateStandingQuery(
    includeSleeping: Option[Boolean],
    namespaceId: NamespaceId,
    wakeUpParallelism: Int
  ): Future[Unit] =
    graph
      .standingQueries(namespaceId)
      .fold(Future.successful[Unit](())) {
        _.propagateStandingQueries(Some(wakeUpParallelism).filter(_ => includeSleeping.getOrElse(false)))
          .map(_ => ())(ExecutionContext.parasitic)
      }

  private def validateOutputDef(outputDef: StandingQueryResultOutputUserDef): Option[NonEmptyList[ErrorString]] =
    outputDef match {
      case k: WriteToKafka => KafkaSettingsValidator.validateOutput(k.kafkaProperties)
      case _ => None
    }
  def addSQOutput(
    name: String,
    outputName: String,
    namespaceId: NamespaceId,
    sqResultOutput: StandingQueryResultOutputUserDef
  ): Future[Either[CustomError, Unit]] = graph.requiredGraphIsReadyFuture {
    validateOutputDef(sqResultOutput) match {
      case Some(errors) =>
        Future.successful(Left(BadRequest(s"Cannot create output `$outputName`: ${errors.toList.mkString(",")}")))

      case _ =>
        app
          .addStandingQueryOutput(name, outputName, namespaceId, sqResultOutput)
          .map {
            case Some(false) => Left(BadRequest(s"There is already a standing query output named '$outputName'"))
            case _ => Right(())
          }(graph.shardDispatcherEC)
    }
  }

  def deleteSQOutput(
    name: String,
    outputName: String,
    namespaceId: NamespaceId
  ): Future[Option[StandingQueryResultOutputUserDef]] = graph.requiredGraphIsReadyFuture {
    app.removeStandingQueryOutput(name, outputName, namespaceId)
  }
  def createSQ(
    name: String,
    namespaceId: NamespaceId,
    sq: StandingQueryDefinition
  ): Future[Either[CustomError, Option[Unit]]] =
    graph.requiredGraphIsReadyFuture {
      try app
        .addStandingQuery(name, namespaceId, sq)
        .map {
          case false => Left(BadRequest(s"There is already a standing query named '$name'"))
          case true => Right(Some(()))
        }(graph.nodeDispatcherEC)
        .recoverWith { case _: NamespaceNotFoundException =>
          Future.successful(Right(None))
        }(graph.nodeDispatcherEC)
      catch {
        case iqp: InvalidQueryPattern => Future.successful(Left(BadRequest(iqp.message)))
        case cypherException: CypherException => Future.successful(Left(BadRequest(cypherException.pretty)))
      }
    }

  def deleteSQ(name: String, namespaceId: NamespaceId): Future[Option[RegisteredStandingQuery]] =
    app.cancelStandingQuery(name, namespaceId)

  def getSQ(name: String, namespaceId: NamespaceId): Future[Option[RegisteredStandingQuery]] =
    app.getStandingQuery(name, namespaceId)
}

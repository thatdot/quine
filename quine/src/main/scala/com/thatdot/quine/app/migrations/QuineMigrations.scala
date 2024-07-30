package com.thatdot.quine.app.migrations

import scala.concurrent.{ExecutionContext, Future}

import com.thatdot.quine.app.migrations.instances.MultipleValuesRewrite
import com.thatdot.quine.graph.{NamespaceId, StandingQueryPattern}
import com.thatdot.quine.migrations.MigrationError
import com.thatdot.quine.persistor.cassandra.{CassandraPersistor, StandingQueryStatesDefinition}
import com.thatdot.quine.persistor.{
  EmptyPersistor,
  InMemoryPersistor,
  PrimePersistor,
  RocksDbPersistor,
  WrappedPersistenceAgent
}
import com.thatdot.quine.util.ComputeAndBlockingExecutionContext
import com.thatdot.quine.util.Log._

/** [[Migration.Apply]] instances for the Quine application. These may be reused by the other Quine-based applications
  * if appropriate.
  */
object QuineMigrations {

  class ApplyMultipleValuesRewrite(val persistor: PrimePersistor, val namespaces: Set[NamespaceId])
      extends Migration.Apply[MultipleValuesRewrite.type] {
    val migration = MultipleValuesRewrite

    def run()(implicit
      ecs: ComputeAndBlockingExecutionContext
    ): Future[Either[MigrationError, Unit]] = {
      val defaultPersistor = WrappedPersistenceAgent.unwrap(persistor.getDefault)
      // check the persistor type before doing any persistor lookups -- if it's a persistor that _can't_ have
      // relevant state, we can skip the lookups.
      val persistorTypeMayNeedMigration: Boolean = defaultPersistor match {
        // persistors with no backing storage can't have any relevant state to migrate
        case _: EmptyPersistor => false
        case _: InMemoryPersistor => false
        case _ => true
      }
      if (!persistorTypeMayNeedMigration) Future.successful(Right(()))
      else {
        // The migration is relevant to the configured persistor, so we need to inspect the persistor's state
        // to determine if the migration is necessary.
        val needsMigrationFut = ApplyMultipleValuesRewrite.needsMigration(persistor, namespaces)
        needsMigrationFut
          .map(_.flatMap { needsMigration =>
            if (!needsMigration) {
              Right(())
            } else {
              // prefix to the (persistor-dependent) message
              val adviceContext =
                "Incompatible MultipleValues standing query states detected from a previous version of Quine."
              // persistor-dependent message
              val userAdvice = ApplyMultipleValuesRewrite.persistorSpecificAdvice(persistor, namespaces)
              // suffix to the (persistor-dependent) message
              val changeReference =
                "See https://github.com/thatdot/quine/releases/tag/v1.7.0 for complete change notes"
              Left(
                MigrationError.UserInterventionRequired(
                  safe"${Safe(adviceContext)} ${Safe(userAdvice)}" + safe"\n" +
                  safe"${Safe(changeReference)}"
                )
              )
            }
          })(ecs.nodeDispatcherEC)
      }
    }
  }
  object ApplyMultipleValuesRewrite {

    private[this] def anyMultipleValuesQueriesRegistered(persistor: PrimePersistor)(implicit
      ecs: ComputeAndBlockingExecutionContext
    ): Future[Boolean] =
      persistor
        .getAllStandingQueries()
        .map(
          _.values.flatten // consider all sqs from all namespaces
            .map(_.queryPattern)
            .exists {
              case _: StandingQueryPattern.MultipleValuesQueryPattern => true
              case _ => false
            }
        )(ecs.nodeDispatcherEC)

    private[this] def anyNamespaceHasMultipleValuesStates(
      persistor: PrimePersistor,
      namespaces: Set[NamespaceId]
    ): Future[Boolean] =
      namespaces.toSeq
        .flatMap(persistor.apply)
        .foldLeft(Future.successful(false))((foundMultipleValuesStatesFut, nextPersistor) =>
          foundMultipleValuesStatesFut
            .flatMap {
              case true => Future.successful(true)
              case false => nextPersistor.containsMultipleValuesStates()
            }(ExecutionContext.parasitic)
            .recoverWith { case err: Throwable =>
              Future.failed(
                new MigrationError.PersistorError(
                  err
                )
              )
            }(ExecutionContext.parasitic)
        )

    /** Perform persistor lookups to see if the persistor contains any multiplevalues-related state
      */
    def needsMigration(persistor: PrimePersistor, namespaces: Set[NamespaceId])(implicit
      ecs: ComputeAndBlockingExecutionContext
    ): Future[Either[MigrationError, Boolean]] =
      anyMultipleValuesQueriesRegistered(persistor)
        .flatMap {
          case true => Future.successful(Right(true))
          case false =>
            anyNamespaceHasMultipleValuesStates(persistor, namespaces)
              .map(Right(_))(ExecutionContext.parasitic)
        }(ecs.nodeDispatcherEC)
        .recover { case err: MigrationError =>
          Left(err)
        }(ExecutionContext.parasitic)

    private def persistorSpecificAdvice(persistor: PrimePersistor, namespaces: Set[NamespaceId]) =
      WrappedPersistenceAgent.unwrap(persistor.getDefault) match {
        case cass: CassandraPersistor =>
          // In case we don't have a keyspace connected, we can still give a sensible message and let the user
          // do their own string substitution
          val (keyspace, keyspaceExplanation) =
            cass.keyspace.fold(
              "<keyspace>" -> "\n(where <keyspace> is the name of your configured keyspace)."
            )(
              _ -> ""
            )

          """In order to continue using your persisted data in Cassandra, please run the previous version of
            |Quine and use the API to remove all standing queries with the `MultipleValues` mode. Then,
            |before starting the updated version of Quine, remove all incompatible feature-specific data from
            |the Cassandra persistor using the following CQL command[s]:""".stripMargin.replace('\n', ' ') +
          namespaces.toSeq
            .map(new StandingQueryStatesDefinition(_).name)
            .map(tableName => s"  TRUNCATE TABLE $keyspace.$tableName;")
            .mkString(start = "\n", sep = "\n", end = keyspaceExplanation)
        case _: RocksDbPersistor =>
          s"""The RocksDB-type persistor does not support side-channel updates, so migration is not possible at
             |this time. Please remove the following directory/directories before restarting Quine:
             |""".stripMargin.replace('\n', ' ').trim +
            namespaces.toSeq
              .map(persistor.apply)
              .collect { case Some(namespaced) =>
                val filePath =
                  WrappedPersistenceAgent.unwrap(namespaced).asInstanceOf[RocksDbPersistor].filePath
                s"  $filePath"
              }
              .mkString(start = "\n", sep = "\n", end = "")
        case badNewsPersistor =>
          s"""The ${badNewsPersistor.getClass.getName}-type persistor does not
             |support side-channel updates, so no migration is possible at this time. Please remove
             |the persistor's stored data and restart Quine.""".stripMargin
      }
  }
}

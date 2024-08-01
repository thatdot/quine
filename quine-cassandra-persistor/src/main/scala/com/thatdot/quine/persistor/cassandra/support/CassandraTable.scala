package com.thatdot.quine.persistor.cassandra.support

import scala.collection.{Factory, immutable}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, Row, SimpleStatement, Statement}
import com.datastax.oss.driver.api.core.data.GettableById

abstract class CassandraTable(
  session: CqlSession,
  firstRowStatement: SimpleStatement,
  dropTableStatement: SimpleStatement
) {

  /** Does the table have any rows?
    */
  def isEmpty(): Future[Boolean] = yieldsResults(firstRowStatement).map(!_)(ExecutionContext.parasitic)

  def delete(): Future[Unit] = executeFuture(dropTableStatement)

  protected def pair[A, B](columnA: CassandraColumn[A], columnB: CassandraColumn[B])(row: GettableById): (A, B) =
    (columnA.get(row), columnB.get(row))

  /** Helper method for wrapping Java Reactive Streams CQL execution in Pekko Streams
    *
    * @param statement A CQL statement to be executed - either prepared or not.
    * @return a Pekko Source of result rows - intended for things that return multiple results
    */
  final protected def executeSource(statement: Statement[_]): Source[ReactiveRow, NotUsed] =
    Source.fromPublisher(session.executeReactive(statement))

  /** Run a CQL query and collect the results to a Scala collection.
    *
    * @param statement The CQL query to execute.
    * @param rowFn A function to apply to transform each returned Cassandra row.
    * @tparam A The desired type of the elements.
    * @tparam C The collection type returned - e.g. {{{List[String]}}}
    * @return a Scala collection containing the result of applying rowFn to the returned Cassandra rows.
    */
  final protected def executeSelect[A, C](statement: Statement[_])(rowFn: Row => A)(implicit
    materializer: Materializer,
    cbf: Factory[A, C with immutable.Iterable[_]]
  ): Future[C] =
    executeSource(statement).map(rowFn).named("cassandra-select-query").runWith(Sink.collection)

  /** Same as {{{executeSelect}}}, just with a {{{CassandraColumn.get}}} as the {{{rowFn}}}
    *
    * @param statement The CQL query to execute.
    * @param col Which column to select from the Cassandra rows.
    * @tparam A The type of the selected column.
    * @tparam C The collection type returned - e.g. {{{List[String]}}}
    * @return a Scala collection containing the selected column.
    */
  final protected def selectColumn[A, C](statement: Statement[_], col: CassandraColumn[A])(implicit
    materializer: Materializer,
    cbf: Factory[A, C with immutable.Iterable[_]]
  ): Future[C] =
    executeSelect(statement)(col.get)

  final protected def selectColumns[A, B, C](
    statement: Statement[_],
    colA: CassandraColumn[A],
    colB: CassandraColumn[B]
  )(implicit
    materializer: Materializer,
    cbf: Factory[(A, B), C with immutable.Iterable[_]]
  ): Future[C] =
    executeSelect(statement)(pair(colA, colB))

  final private def queryFuture[A](statement: Statement[_], f: AsyncResultSet => A): Future[A] =
    session.executeAsync(statement).asScala.map(f)(ExecutionContext.parasitic)

  final private def singleRow[A](col: CassandraColumn[A])(resultSet: AsyncResultSet): Option[A] =
    Option(resultSet.one()).map(col.get)
  final protected def queryOne[A](statement: Statement[_], col: CassandraColumn[A]): Future[Option[A]] =
    queryFuture(statement, singleRow(col))
  final protected def queryCount(statement: Statement[_]): Future[Int] = queryFuture(
    statement,
    { resultSet =>
      // The return type of "SELECT COUNT" in Cassandra is a bigint, aka int64 / long
      // We convert it to int because this will probably time out above around 500,000 rows anyways.
      val count = resultSet.one().getLong(0)
      if (count <= Int.MaxValue) count.toInt else sys.error(s"Row count $count too big to fit in Int")
    }
  )

  /** Helper method for converting no-op results to {{{Future[Unit]}}}
    *
    * @param statement A CQL statemment to be executed - either prepared or not.
    * @return Unit - intended for INSERT or CREATE TABLE statements that don't return a useful result.
    */
  final protected def executeFuture(statement: Statement[_]): Future[Unit] =
    queryFuture(statement, _ => ())

  /** Helper function to evaluate if a statement yields at least one result
    * @param statement The statement to test
    * @return a future that returns true iff the provided query yields at least 1 result
    */
  final protected def yieldsResults(statement: Statement[_]): Future[Boolean] =
    session.executeAsync(statement).thenApply[Boolean](_.currentPage.iterator.hasNext).asScala
}

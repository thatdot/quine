package com.thatdot.quine.persistor.cassandra.support

import scala.jdk.CollectionConverters._

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder
import com.datastax.oss.driver.api.core.data.GettableById
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, literal}
import com.datastax.oss.driver.api.querybuilder.relation.{ColumnRelationBuilder, Relation}
import com.datastax.oss.driver.api.querybuilder.term.Term

final case class CassandraColumn[A](name: CqlIdentifier, codec: TypeCodec[A]) {
  def cqlType: DataType = codec.getCqlType
  private def set(bindMarker: CqlIdentifier, value: A)(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    statementBuilder.set(bindMarker, value, codec)
  def setSeq(values: Seq[A])(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    statementBuilder.set(name, values, CassandraCodecs.listCodec(codec))
  def setSet(values: Set[A])(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    statementBuilder.set(name, values, CassandraCodecs.setCodec(codec))
  def set(value: A)(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    set(name, value)(statementBuilder)
  def setLt(value: A)(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    set(ltMarker, value)(statementBuilder)
  def setGt(value: A)(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    set(gtMarker, value)(statementBuilder)

  def get(row: GettableById): A = row.get[A](name, codec)

  private def prefixCqlId(prefix: String): CqlIdentifier = CqlIdentifier.fromInternal(prefix + name.asInternal)
  def gtMarker: CqlIdentifier = prefixCqlId("gt_")
  def ltMarker: CqlIdentifier = prefixCqlId("lt_")

  // Relation builders for use when constructing prepared statements.
  object is {
    private def relBuilder: ColumnRelationBuilder[Relation] = Relation.column(name)
    def eq: Relation = relBuilder.isEqualTo(bindMarker(name))
    def lte: Relation = relBuilder.isLessThanOrEqualTo(bindMarker(ltMarker))
    def gte: Relation = relBuilder.isGreaterThanOrEqualTo(bindMarker(gtMarker))
    // The usual "templated" prepared statement variant
    def in: Relation = relBuilder.in(bindMarker(name))
    // The inline literal variant - to put a literal into the statement instead of a bindMarker.
    def in(values: Iterable[A]): Relation = relBuilder.in(values.map(v => literal(v, codec): Term).asJava)
  }
}

object CassandraColumn {
  def apply[A](name: String)(implicit codec: TypeCodec[A]): CassandraColumn[A] =
    new CassandraColumn(CqlIdentifier.fromCql(name), codec)
}

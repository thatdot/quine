package com.thatdot.quine.graph.cypher

sealed abstract class Type {

  /** Pretty-print the type
    *
    * @note this is defined to match the openCypher spec as much as possible
    */
  final def pretty: String = this match {
    case Type.Number => "NUMBER"
    case Type.Integer => "INTEGER"
    case Type.Floating => "FLOAT"
    case Type.Bool => "BOOLEAN"
    case Type.Str => "STRING"
    case Type.List(of) => s"LIST OF ${of.pretty}"
    case Type.Map => "MAP"
    case Type.Null => "NULL"
    case Type.Bytes => "BYTES"
    case Type.Node => "NODE"
    case Type.Relationship => "RELATIONSHIP"
    case Type.Path => "PATH"
    case Type.LocalDateTime => "LOCALDATETIME"
    case Type.DateTime => "DATETIME"
    case Type.Duration => "DURATION"
    case Type.Date => "DATE"
    case Type.Time => "TIME"
    case Type.LocalTime => "LOCALTIME"
    case Type.Anything => "ANY"
  }
}
object Type {
  case object Number extends Type
  case object Integer extends Type
  case object Floating extends Type
  case object Bool extends Type
  case object Str extends Type
  final case class List(of: Type) extends Type
  case object Map extends Type
  case object Null extends Type
  case object Bytes extends Type
  case object Node extends Type
  case object Relationship extends Type
  case object Path extends Type
  case object LocalDateTime extends Type
  case object DateTime extends Type
  case object Duration extends Type
  case object Date extends Type
  case object Time extends Type
  case object LocalTime extends Type
  case object Anything extends Type

  val ListOfAnything: List = List(Anything)

  final def number() = Number
  final def integer() = Integer
  final def floating() = Floating
  final def bool() = Bool
  final def str() = Str
  final def list(of: Type = Anything): List = List(of)
  final def map() = Map
  final def nullType() = Null
  final def bytes() = Bytes
  final def node() = Node
  final def relationship() = Relationship
  final def path() = Path
  final def localDateTime() = LocalDateTime
  final def dateTime() = DateTime
  final def date() = Date
  final def time() = Time
  final def localTime() = LocalTime
  final def duration() = Duration
  final def anything() = Anything
}

package com.thatdot.quine.language.server

sealed trait SimpleTrie

object SimpleTrie {
  case class Node(children: Map[Char, SimpleTrie]) extends SimpleTrie
  case object Leaf extends SimpleTrie
}

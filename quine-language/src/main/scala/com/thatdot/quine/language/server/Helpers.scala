package com.thatdot.quine.language.server

object Helpers {

  def addItem(name: String, data: SimpleTrie): SimpleTrie = {
    def go(xs: List[Char], level: SimpleTrie): SimpleTrie = xs match {
      case h :: t =>
        level match {
          case SimpleTrie.Node(children) =>
            SimpleTrie.Node(children + (h -> (children.get(h) match {
              case Some(child) => go(t, child)
              case None => go(t, SimpleTrie.Leaf)
            })))
          case SimpleTrie.Leaf => SimpleTrie.Node(Map(h -> go(t, SimpleTrie.Leaf)))
        }
      case Nil => SimpleTrie.Leaf
    }
    go(name.toList, data)
  }
}

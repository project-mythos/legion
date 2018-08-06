package Legion

import Gossip.Messages.Peer
import Enki_DT.{SetCRDT, ORSet}


import Enki.PipeOps._
import Enkidu.Node




case class View(local: Node, membership: ORSet[Node]) {
  def neighbors = {
    (ORSet.query(membership) - local).toVector
  }

  def commit(m: ORSet[Node]) = {
    this.copy(membership = m)
  }

  def join(peer: Node) = {
    val ns = ORSet.add(membership, peer)
    commit(ns)
  }


  def leave(peer: Node) = {
    val ns = ORSet.delete(membership, peer)
    commit(ns)
  }

  def isEmpty() = neighbors.isEmpty
  def notEmpty() = neighbors.isEmpty == false

  def memberlist = {
    ORSet.query(membership)
  }

}





object View {

  def apply(local: Node): View = {
    val mlist = ORSet.empty[Node](local.id.toString) |> {os => ORSet.add(os, local) }
    View(local, mlist)
  }

}







package Gossip.Messages 

import java.net.InetSocketAddress
import Enki.PipeOps._
import Enkidu.Mux._
import Enki.PipeOps._
import java.util.UUID

import Enkidu.Node
import com.trueaccord.scalapb.TypeMapper
import Enki_DT.Proto.PBBridge
import scala.language.implicitConversions

object Node2 extends PBBridge[Node, Peer] {

  val companion = Peer

  def toPB(node: Node) = {
    Peer(node.id.toString, node.host, node.port)
  }


  def fromPB(peer: Peer) = {
    Node(peer.id.toLong, peer.host, peer.port)
  }



  class Encoder(n: Node) {
    def toByteArray() = toPB(n).toByteArray
  }


  implicit def encodeable(n: Node) = new Encoder(n)
  //def parseFrom(b: Array[Byte]) = ( Peer.parseFrom(b) |> fromPB )

}


case class Rumor(
  id: String, round: Int, from: Node
) {

  def nextRound = this.copy(round = round + 1)

}


object Rumor {

  def apply(peer: Node): Rumor = {
    val id = UUID.randomUUID().toString
    Rumor(id, 1, peer)
  }


  def zero(peer: Node) = {
    val id = UUID.randomUUID().toString
    Rumor(id, 0, peer)
  }


}




class RumorSession[T](M: MSG[T]) {




  def encode(tmsg: T, rumor: Rumor) = {

    val headers = List(
      ("rumor-id", rumor.id),
      ("gossip-type", "rumor"),
      ("round", rumor.round.toString),
      ("from", Node.toString(rumor.from) )
    )

    
    val h1 = M.headers(tmsg) |> { h => Headers.addBatch(h, headers) }
    M.withHeaders(tmsg, h1)

  }



  def decode(tmsg: T): Option[Rumor] = {
    val h = M.headers(tmsg)

    val res = (
      Headers.get(h, "rumor-id"),
      Headers.get(h, "round") map {x => x.toInt},
      Headers.get(h, "from") map {a => Node.fromString(a) },
    )

    res match {

      case (Some(id), Some(round), Some(from)) =>
        Some( Rumor(id, round, from) )

      case _ => None
    }

  }



}



case class Exchange(id: String, from: Node)

class ExchangeSession[T](M: MSG[T]) {

  def embed(msg: T, exch: Exchange) = {
    val headers = List(
      ("exchange-id", exch.id),
      ("from", Node.toString(exch.from) )
    )

    val h1 = M.headers(msg) |> {h => Headers.addBatch(h, headers) }
    M.withHeaders(msg, h1)
  }



  def decode(msg: T) = {
    val hdrs = M.headers(msg)

    val o = (
      Headers.get(hdrs, "exchange-id"),
      Headers.get(hdrs, "from")
    )

    o match {

      case ( Some(id), Some(addr) ) =>

        Some(
          Exchange(id, Node.fromString(addr) )
        )

      case _ => None
    }

  }



}

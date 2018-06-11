package Gossip.Messages 

import java.net.InetSocketAddress
import Enkidu.Mux._
import Enki.PipeOps._
import java.util.UUID



object Addr {
  def toString(addr: Peer) = {
    s"${addr.id}@${addr.host}:${addr.port}"
  }

  def fromString(s: String) = {
    val id :: tl = s.split("@").toList

    val toks = tl(0).split(":").toList
    Peer(id, toks(0), toks(1).toInt)
  }

  def toSocketAddress(a: Peer) = new InetSocketAddress(a.host, a.port) 


}







case class Rumor(
  id: String, round: Int, from: Peer
) {
  def nextRound = this.copy(round = round + 1)
}


object Rumor {

  def apply(peer: Peer): Rumor = {
    val id = UUID.randomUUID().toString
    Rumor(id, 1, peer)
  }


  def zero(peer: Peer) = {
    val id = UUID.randomUUID().toString
    Rumor(id, 1, peer)
  }


}



class RumorSession[T](M: MSG[T]) {




  def encode(tmsg: T, rumor: Rumor) = {

    val headers = List(
      ("rumor-id", rumor.id),
      ("gossip-type", "rumor"),
      ("round", rumor.round.toString),
      ("from", Addr.toString(rumor.from) )
    )

    
    val h1 = M.headers(tmsg) |> { h => Headers.addBatch(h, headers) }
    M.withHeaders(tmsg, h1)

  }



  def decode(tmsg: T): Option[Rumor] = {
    val h = M.headers(tmsg)

    val res = (
      Headers.get(h, "rumor-id"),
      Headers.get(h, "round") map {x => x.toInt},
      Headers.get(h, "from") map {a => Addr.fromString(a) },
    )

    res match {

      case (Some(id), Some(round), Some(from)) => Some(
        Rumor(id, round, from)
      )

      case _ => None

    }

  }



}



case class Exchange(id: String, from: Peer)

class ExchangeSession[T](M: MSG[T]) {

  def embed(msg: T, exch: Exchange) = {
    val headers = List(
      ("exchange-id", exch.id),
      ("from", Addr.toString(exch.from) )
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
          Exchange(id, Addr.fromString(addr) )
        )

      case _ => None
    }

  }



}

package Legion.AntiEntropy
import legion.operators.PipeOps._
import Legion.PeerService

import Legion.Gossip._
import java.util.UUID
import Enkidu.{Flow, Connection}
import Enkidu.Mux._
import com.google.protobuf.ByteString
import com.twitter.util._

trait Sync[T] {

  def merge(l: T, r: T): T
  def encode(t: T): Array[Byte]
  def decode(bytes: Array[Byte]): T

}




class AntiEntropy[T](S: Sync[T], PS: PeerService) {

  def mkExchange(payload: ByteString): Exchange = {
    val id = UUID.randomUUID().toString
    Exchange(id, PS.local, payload)
  }


  def mkExchange(payload: Array[Byte]): Exchange = {
    val bs = ByteString.copyFrom(payload)
    mkExchange(bs)
  }

 
  def decodeBody(e: Exchange): T = {
    val body = e.payload.toByteArray()
    S.decode(body)
  }
 

  def sender(path: List[String], data: T): Future[T] = {

    val f = PS.selectOne() |> PS.connect
    val myPayload = mkExchange( S.encode(data) ).toByteArray

    val req = TMSG(path, myPayload) 

    f flatMap { flow => Connection.send_recv(flow, req) } map {rep =>
      val r = rep.payload |> Exchange.parseFrom |> decodeBody
      S.merge(data, r)
    }


  }


  def receiver(flow: Flow[RMSG, TMSG])(data: T, req: TMSG) = {
    val other_s = req.payload |> Exchange.parseFrom |> decodeBody
    val newState = S.merge(data, other_s)

    val rep =
      S.encode(newState) |> mkExchange |>  {x => RMSG(x.toByteArray)}


    flow.write(rep) map {x => newState}
  }

}

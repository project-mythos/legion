package Legion.AntiEntropy

import Enki.PipeOps._
import Legion.{PeerView, Sampler}

import Gossip.Messages._
import java.util.UUID
import Enkidu.{Flow, Connection}
import Enkidu.Mux._
import com.google.protobuf.ByteString
import com.twitter.util._
import Enki.Path


trait Sync[T] {

  def merge(l: T, r: T): T
  def encode(t: T): Array[Byte]
  def decode(bytes: Array[Byte]): T
}


class Synchronizer[T](S: Sync[T], PS: Sampler) {

  val TS = new ExchangeSession(TMSG)
  val RS = new ExchangeSession(RMSG)

  def mkExchange(view: PeerView) = {
    val id = UUID.randomUUID().toString
    Exchange(id, view.local)
  }


  def tsync(view: PeerView, t: T, tmsg: TMSG): Future[T] = {

    val f = PS.selectOne(view) |> PS.connect
    val payload = S.encode(t)

    val req = TS.embed(tmsg, mkExchange(view) ) |> {
      m => TMSG.withBody(m, payload)
    }

    
    f flatMap { flow =>
      Connection.send_recv(flow, req)
    } map {rep =>

      val r = rep.payload |> S.decode 
      S.merge(t, r)

    } ensure {x: T =>
      f onSuccess { flow => flow.close() }
    }


  }


  def rsync(view: PeerView, flow: Flow[RMSG, TMSG])(exch: Exchange, data: T, req: TMSG): Future[T] = {

    val other_s = req.payload |> S.decode
    val newState = S.merge(data, other_s)

    val r = RMSG( Array[Byte]() )
    val exch1 = exch.copy(from=view.local)

    val rep = RS.embed(r, exch1) |> {r1 => RMSG.withBody(r1 , S.encode(newState) ) }

    flow.write(rep) map {x => newState} 
  }

}

/*
class ScheduledAE[T](PS: Sampler, Src: Source[T], sync: Sync[T]) {

  val AE = new Syncer(sync, PS)

  def schedule(interval: Duration, path: Path.T) = {
    def exchanger() = {
      AE.sender(Src.view, path, Src.get) onSuccess {merged => Src.become(merged)} 
    }

    val timer = new JavaTimer()
    timer.schedule(interval)(exchanger)
    
  }



}
 
*/


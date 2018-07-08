package Legion.AntiEntropy

import Enki.PipeOps._
import Legion.{View, Sampler}

import Gossip.Messages._
import java.util.UUID
import Enkidu.{Flow, Connection}
import Enkidu.Mux._
import com.google.protobuf.ByteString
import com.twitter.util._
import Enki.Path


trait Contents[T] {

  def merge(l: T, r: T): T
  def encode(t: T): Array[Byte]
  def decode(bytes: Array[Byte]): T
}






class Sync[T](val C: Contents[T], PS: Sampler) {

  val TS = new ExchangeSession(TMSG)
  val RS = new ExchangeSession(RMSG)

  def mkExchange(view: View) = {
    val id = UUID.randomUUID().toString
    Exchange(id, view.local)
  }


  def tsync(view: View, t: T, tmsg: TMSG): Future[T] = {

    val payload = C.encode(t)

    val req = TS.embed(tmsg, mkExchange(view) ) |> {
      m => TMSG.withBody(m, payload)
    }

    val peer = PS.selectOne(view)

    PS.connect(peer) { flow =>
      Connection.send_recv(flow, req)
    } map {rep =>

      val r = rep.payload |> C.decode 
      C.merge(t, r)
    }

  }


  def rsync(flow: Flow[RMSG, TMSG], data: T, req: TMSG): Future[T] = {

    val other_s = req.payload |> C.decode
    val newState = C.merge(data, other_s)

    val rep = RMSG(C.encode(newState) )
    flow.write(rep) map {x => newState} 
  }


  def rsync_raw(flow: Flow[RMSG, TMSG], l: T, r: T): Future[T] = {
    val newState = C.merge(l, r)
    val rep = RMSG(C.encode(newState) )

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



trait State[T] {
  def become(t: T): Unit 
  def get(): T 
}




/**
  * Utilities to create services that use anti entropy 
*/
class SyncHelpers[T](
  view: () => View,
  ST: State[T], 
  Sampler: Sampler,
  C: Contents[T]
) {

  val Sync = new Sync(C, Sampler) 
  val timer = new JavaTimer() 


  /** A generic callback function for servers doing sync */
  def handle_sync(flow: Flow[RMSG, TMSG], req: TMSG) = {
    Sync.rsync(flow, ST.get, req) map { x => ST.become(x) }
  }


  /** A process that schedules synchronization*/
  def scheduled_sync(interval: Duration, tmsg: TMSG) = {

    def op() = {
      if ( view().notEmpty) {

        Sync.tsync(view(), ST.get(), tmsg) onSuccess {v1 =>
          ST.become(v1)
        }
      }
    }

    timer.schedule(interval)(op)
  }


}

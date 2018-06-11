package Legion.Epidemic 

import Enkidu.{Flow, Connection}
import Enkidu.Mux._
import Gossip.Messages._
import java.util.UUID

import bloomfilter.mutable.BloomFilter
import Legion.{PeerView, Sampler}

import com.twitter.util.{Future, Promise}
import com.google.protobuf.ByteString

trait SIR

object SIR {


  val RS = new RumorSession(TMSG)

  def check(conf: Config, rumor: Rumor): SIR = {
    val bloom = conf.bloom

    val seen = bloom.mightContain(rumor.id)
    val expired = rumor.round >= conf.maxRounds

    (seen, expired) match {
      case (true, _) => Infected

      case (false, expired) if (expired == conf.maxRounds) =>
        Susceptible

      case (false, false) => Contagious
    }
  }


  case object Infected extends Throwable with SIR
  case object Contagious extends SIR
  case object Susceptible extends SIR
}



case class Config(
  PS: Sampler,
  fanout: Int,
  maxRounds: Int,
  bloom: BloomFilter[String]
) {

  def markSeen(id: String) = {
    synchronized {bloom.add(id)}
  }

}



class Disseminator(config: Config) {


  val conf = config 

  val RumorSession = new RumorSession(TMSG) 


  def spread(view: PeerView, rumor: Rumor, tmsg: TMSG): Future[Unit] = {
    markSeen(rumor)

    val msg = RumorSession.encode(tmsg, rumor )

    val p = Promise[Unit]() 

    val spread = conf.PS.select(view, conf.fanout) map { peer =>
      conf.PS.connect(peer) map {flow => Connection.fire_forget(flow, msg) }
    }

    Future.collectToTry(spread) map {x => () }

  }



  def forward(view: PeerView, rumor: Rumor, tmsg: TMSG) = {
    val r1 = rumor.nextRound.copy(from=view.local)
    spread(view, r1, tmsg)
  }


  def markSeen(rumor: Rumor) = {
    conf.bloom.add(rumor.id)
  }


 

  def handleRumor[T](
    view: PeerView,
    tmsg: TMSG,
    process: (Rumor, TMSG) => Future[T]
  ): Future[T]  = {
    
    Future { RumorSession.decode(tmsg).get } flatMap { rumor =>

      val status = SIR.check(conf, rumor)

      status match {

        case SIR.Contagious =>
          forward(view, rumor, tmsg)
          process(rumor, tmsg)

        case SIR.Susceptible =>
          process(rumor, tmsg)

        case SIR.Infected =>
          Future.exception( SIR.Infected )
      }
    }

  }

}




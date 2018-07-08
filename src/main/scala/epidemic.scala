package Legion.Epidemic 

import Enkidu.{Flow, Connection}
import Enkidu.Mux._
import Gossip.Messages._
import java.util.UUID

import bloomfilter.mutable.BloomFilter
import Legion.{View, Sampler}

import com.twitter.util.{Future, Promise}
import com.google.protobuf.ByteString


trait SIR

object SIR {

  def is_expired(conf: Config, rumor: Rumor) = {
    conf.maxRounds match {
      case Some(x) => rumor.round >= x
      case None => false 
    }

  }

  def is_susceptible(conf: Config, rumor: Rumor) = {
    conf.maxRounds match {
      case Some(x) => x == rumor.round
      case None => false
    }
  }

  val RS = new RumorSession(TMSG)

  def check(conf: Config, rumor: Rumor): SIR = {
    val bloom = conf.bloom

    val seen = bloom.mightContain(rumor.id)
    val expired = is_expired(conf, rumor)

    (seen, expired) match {
      case (true, _) => Infected

      case (false, true) if ( is_susceptible(conf, rumor) ) =>
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
  maxRounds: Option[Int] = None,
  bloom: BloomFilter[String]
) {

  def markSeen(id: String) = {
    synchronized {bloom.add(id)}
  }

}

object Config {

  def default_bloom() = BloomFilter[String](1000, .001) 
  def apply(s: Sampler): Config = {
    Config(s, 3, None, default_bloom)
  }

  def apply(s: Sampler, fanout: Int): Config = {
    Config(s, fanout, None, default_bloom )
  }

  def apply(s: Sampler, fanout: Int, max: Int): Config = {
    Config(s, fanout, Some(max), default_bloom)
  }

  def apply(
    PS: Sampler,
    fanout: Int,
    max: Int,
    bloom: BloomFilter[String]
  ): Config = {
    Config(PS, fanout, Some(max), bloom )
  }
}



class Disseminator(config: Config) {


  val conf = config 

  val RumorSession = new RumorSession(TMSG) 


  def spread(view: View, rumor: Rumor, tmsg: TMSG): Future[Unit] = {
    markSeen(rumor)

    val msg = RumorSession.encode(tmsg, rumor )

    val p = Promise[Unit]() 

    val spread = conf.PS.select(view, conf.fanout) map { peer =>
      conf.PS.connect(peer) {flow => Connection.fire_forget(flow, msg) }
    }

    Future.collectToTry(spread) map {x => () }

  }



  def forward(view: View, rumor: Rumor, tmsg: TMSG) = {
    val r1 = rumor.nextRound.copy(from=view.local)
    spread(view, r1, tmsg)
  }


  def markSeen(rumor: Rumor) = {
    conf.bloom.add(rumor.id)
  }


 

  def handleRumor[T](
    view: View,
    tmsg: TMSG,
    rumor: Rumor
  )(process: TMSG => Future[T] ): Future[T]  = {

    val status = SIR.check(conf, rumor)

    status match {

      case SIR.Contagious =>
        forward(view, rumor, tmsg)
        process(tmsg)

      case SIR.Susceptible =>
        process(tmsg)

      case SIR.Infected =>
        Future.exception( SIR.Infected )
    }
  }



}




/** Server side filters to make it easier to write rumor mongering services */
class Filters(
  view: () => View,
  D: Disseminator
) {

  val RumorSession = D.RumorSession 


  val no_rumor_rep = {
    val bytes = "Error no rumor".getBytes("utf8")
    RMSG(bytes)
  }


  /** A simple filter that will produce a rumor if it isn't enclosed and forward the message*/
  def rumor_filter[T](tmsg: TMSG)(f: TMSG => Future[T]) = {
    val rumor = RumorSession.decode(tmsg).getOrElse( Rumor(view().local) )
    D.handleRumor(view(), tmsg, rumor)(f)
  }


  /** For functions that need TMSG + Rumor it will produce one if it's not already in the recieved request*/

  def with_rumor[T](tmsg: TMSG)(f: (TMSG, Rumor) => Future[T]) = {
    val rumor = RumorSession.decode(tmsg).getOrElse( Rumor.zero(view().local)  )
    f(tmsg, rumor)
  }


  /** 
    * For functions that only send a response if they were the first recipient of the rumor
    * It's used along with with_rumor or needs_rumor 
  */

  def first_responder(flow: Flow[RMSG, TMSG], tmsg: TMSG, rumor: Rumor)(f: TMSG => Future[RMSG]) = {
    
    val isFirst = rumor.round == 0
    val rep_f = D.handleRumor(view(), tmsg, rumor)(f)

    rep_f flatMap { rep =>

      if ( isFirst ) {
        flow.write(rep)
      }

      else Future.Done
    }

    
  }



/** 
  A filter that will not process the request unless there is already a rumor enclosed 
*/
  def needs_rumor[T](flow: Flow[RMSG, TMSG], tmsg: TMSG)(f: (TMSG, Rumor) => Future[T]) = {
    val o = RumorSession.decode(tmsg)

    o match {
      case Some(rumor) => f(tmsg, rumor)
      case None =>
        flow.write(no_rumor_rep) flatMap { x =>
          Future.exception(  new Throwable("Bad request" ) )
        }
    }
  }
}

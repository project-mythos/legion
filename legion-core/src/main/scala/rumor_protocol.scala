package legion.rumor_proto

import Enkidu.{Flow, Connection}
import Enkidu.Mux._
import gossip._
import java.util.UUID

import bloomfilter.mutable.BloomFilter
import legion.PeerService


import com.twitter.util.Future
import com.google.protobuf.ByteString


case class Config(
  PS: PeerService,
  fanout: Int,
  maxRounds: Int, 
  bloom: BloomFilter[String]
)


object Disseminator {

  type ClientFlow = Flow[TMSG, RMSG]
  type ServerFlow = Flow[RMSG, TMSG]

}


class Disseminator(conf: Config) {

  def newRumor(payload: ByteString) = {
    val id = UUID.randomUUID().toString
    Rumor(id, 1, conf.PS.local, payload)
  }


  def newRumor(payload: Array[Byte]): Rumor = {
    newRumor(ByteString.copyFrom(payload) )
  }


  def spread(rumor: Rumor, toTMSG: Rumor => TMSG) = {
    val msg = toTMSG(rumor)

    conf.PS.select(conf.fanout) foreach {peer =>
      conf.PS.connect(peer) map {flow => Connection.fire_forget(flow, msg) }  
    }
  }



  def startRumor(payload: ByteString, toTMSG: Rumor => TMSG): Unit = {
    val rumor = newRumor(payload)
    conf.bloom.add(rumor.id)
    spread(rumor, toTMSG) 
  }


  def rumorPredicates(rumor: Rumor) = {
    val bloom = conf.bloom

    val seen = bloom.mightContain(rumor.id)
    val roundExp = rumor.round >= conf.maxRounds 

    val forwardable = (seen || roundExp) != true
    val processable = (seen != true) && (rumor.round <= conf.maxRounds)
    (forwardable, processable)
  }


  def forward(rumor: Rumor, toTMSG: Rumor => TMSG) = {
    val roundI = rumor.round + 1
    val rumor1 = rumor.copy(round = roundI)
    spread(rumor1, toTMSG)
  }





  def handleRumor(
    rumor: Rumor,
    toTMSG: Rumor => TMSG,
    processRumor: Rumor => Unit
  ) = {
    val preds = rumorPredicates(rumor)


    def process(rumor: Rumor) = {
      conf.bloom.add(rumor.id)
      processRumor(rumor)
    }


    val res = preds match {
      case (true, x) =>
        forward(rumor, toTMSG)
        process(rumor)

      case (x, true) =>
        process(rumor)

      case _ => ()

    }

    Future {res}

  }



}




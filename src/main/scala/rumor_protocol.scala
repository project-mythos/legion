package legion.rumor_proto

import enkidu.Flow
import enkidu.mux_proto._
import gossip._
import java.util.UUID

import bloomfilter.mutable.BloomFilter
import legion.core.{Client, PeerService}

import com.twitter.util.Future
import com.google.protobuf.ByteString


case class Config(
  sampler: PeerService,
  fanout: Int,
  maxRounds: Int, 
  bloom: BloomFilter[String]
)


object Disseminator {

  type ClientFlow = Flow[TMSG, RMSG]
  type ServerFlow = Flow[RMSG, TMSG]

  def newRumor(conf: Config, payload: ByteString) = {
    val id = UUID.randomUUID().toString
    Rumor(id, 1, conf.sampler.local, payload)
  }


  def newRumor(conf: Config, payload: Array[Byte]): Rumor = {
    newRumor(conf, ByteString.copyFrom(payload) )
  }


  def spread(conf: Config, rumor: Rumor, toTMSG: Rumor => TMSG) = {
    val msg = toTMSG(rumor)

    conf.sampler.sample(conf.fanout) map {flows =>
      flows.foreach {case (_, flow) => Client.fireForget(flow, msg) } 
    }
  }




  
  def rumorPredicates(config: Config, rumor: Rumor) = {
    val bloom = config.bloom

    val seen = bloom.mightContain(rumor.id)
    val roundExp = rumor.round >= config.maxRounds 


    val forwardable = (seen || roundExp) != true
    val processable = (seen != true) && (rumor.round <= config.maxRounds)
    (forwardable, processable)
  }


  def forward(config: Config, rumor: Rumor, toTMSG: Rumor => TMSG) = {
    val roundI = rumor.round + 1
    val rumor1 = rumor.copy(round = roundI)
    spread(config, rumor1, toTMSG)
  }





  def handleRumor(
    config: Config,
    rumor: Rumor,
    toTMSG: Rumor => TMSG,
    processRumor: Rumor => Unit
  ) = {
    val preds = rumorPredicates(config, rumor)


    def process(rumor: Rumor) = {
      config.bloom.add(rumor.id)
      processRumor(rumor) 
    }


    val res = preds match {
      case (true, true) =>
        forward(config, rumor, toTMSG)
        process(rumor)

      case (x, true) =>
        process(rumor)

      case _ => ()

    }

    Future {res}

  }



}



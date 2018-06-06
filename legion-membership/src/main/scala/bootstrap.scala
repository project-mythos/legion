package Legion.Membership
import Legion.{DefaultPeerService, PeerConnection}
import Enkidu.{Listener, WorkerPool, Connection, Flow}
import Enkidu.Mux._

import io.netty.channel.{ChannelPipeline}
import Legion.Epidemic.{Config, Disseminator}
import io.netty.channel.socket.nio.NioServerSocketChannel

import bloomfilter.mutable.BloomFilter
import com.twitter.util.Duration
import Legion.Gossip._
/**
object Bootstrap {


  type Handler = (Config, TMSG, Flow[RMSG, TMSG]) => Unit

  case class Params(
    local: Peer, fanout: Int,
    max_rounds: Int, handler: Handler,
    detector_interval: Duration
  )

  def initPipeline(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast(new TMSGDecoder())
    pipeline.addLast(new TMSGDecoder() ) 
  }


  def spreadJoin(conf: Config) = {
    val body = conf.sampler.local.toByteString

    def toTMSG(r: Rumor): TMSG = {
      TMSG(Paths.join_rumor, r.toByteArray)
    }

    Disseminator.startRumor(conf, body, toTMSG)
  }


  def bootstrap(pool: WorkerPool)(p: Params, view: PeerView) = {
    val peer_service =  DefaultPeerService.make(pool, view)
    val bloom = BloomFilter[String](1000, 0.01)
    val config = Config(peer_service, p.fanout, p.max_rounds, bloom)

    def extHandler(flow: Flow[RMSG, TMSG]) = {
      flow.read() map {req =>

        req.path match {
          case "membership" :: rest => Handlers.handle(config, req, flow)
          case _ => p.handler(config, req, flow)
        }

      }
    }

    val addr = PeerConnection.toAddress(p.local)

    new FailureDetector(config, p.detector_interval) 
    val listener = new Listener(initPipeline, pool, classOf[NioServerSocketChannel])

    spreadJoin(config)
  }





}
  
**/

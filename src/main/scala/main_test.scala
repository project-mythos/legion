

import enkidu._
import enkidu.mux_proto._
import gossip._

import io.netty.channel.{Channel, ChannelPipeline, ChannelInitializer}
import io.netty.channel.socket.SocketChannel
import io.netty.bootstrap.{ServerBootstrap}

import io.netty.channel.socket.nio.{NioSocketChannel, NioServerSocketChannel}

import legion.{core, rumor_proto, membership}
import bloomfilter.mutable.BloomFilter
import com.twitter.util.Duration

object Listener {


  type Handler = Flow[RMSG, TMSG] => Unit



  def bridge(handler: Handler) = {
    def toFlow(ch: Channel) = Flow.cast[RMSG, TMSG]( new ChannelFlow(ch) )
    new ServerBridge(toFlow, handler)  
  }


  def pipelineInit(pipeline: ChannelPipeline) = {
    pipeline.addLast(new TMSGDecoder() )
    pipeline.addLast(new RMSGEncoder() )
  }


  def channelInitializer(handler: Handler) =  new ChannelInitializer[SocketChannel] {

    val serve = bridge(handler)

    override def initChannel(ch: SocketChannel) = {
      pipelineInit(ch.pipeline)
      ch.pipeline.addLast(serve)
    }
  }



  def serve(pool: WorkerPool, handler: Handler, port: Int) = {

    val b = new ServerBootstrap()
    b
      .group(pool.group)
      .channel(classOf[NioServerSocketChannel])
      .childHandler( channelInitializer(handler) )


    b.bind(port).sync()
  }
  
}

object Run extends App {

  def makeViews(peers: List[Peer] ) = {

     peers map {p =>
      val neighbors = peers.filter(x => x != p) 
      PeerView(p, neighbors)
    }

  }

  val pool = enkidu.WorkerPool.default()
  val bs = core.Client.bootstrap(pool)

  val peers = List(
    Peer("localhost", 3000),
    Peer("localhost", 2022),

    Peer("localhost", 8000),
    Peer("localhost", 3232),
    Peer("localhost", 4222)
  )

  val views = makeViews(peers)

  val confs =
    views.map (x => new core.DefaultPeerService(x, bs) ) map { sampler =>
      val bf = BloomFilter[String](1000, 1)
      rumor_proto.Config(sampler, 2, 3, bf)
    }

  def handler(config: rumor_proto.Config)(flow: Flow[RMSG, TMSG]) = {
    membership.Handlers.handle(config, flow)
  }


  val listeners = confs.map {x =>
    new membership.FailureDetector(x, Duration.fromMilliseconds(100L))
    Listener.serve(pool, handler(x), x.sampler.local.port)
  }


  


  val detector = new membership.FailureDetector(confs.head, Duration.fromMilliseconds(30000L))
  detector.suspectPeer(Peer("localhost", 1000) )
  
  listeners.last.channel().closeFuture().sync()
  

 
}

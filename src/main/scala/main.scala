import Legion._


import Enkidu.{WorkerPool, Mux, Connection, Listener}
import Legion.Epidemic._ 

import io.netty.channel.socket.nio.{NioSocketChannel,  NioServerSocketChannel}
import io.netty.channel.{Channel, ChannelInitializer}

import bloomfilter.mutable.BloomFilter
import Gossip.Messages._
import com.twitter.util.{ Future, Await, RandomSocket}
import Legion.PeerService.{Client, Server}
import Enkidu.Mux._


object Srv extends App {

  val pool = WorkerPool.default

  val clientInitializer = new ChannelInitializer[Channel] {
    def initChannel(c: Channel) = {
      Mux.Pipelines.clientPipeline(c.pipeline)
    }
  }



  val bootstrap = Connection.bootstrap(
    classOf[NioSocketChannel],
    clientInitializer,
    pool 
  )


  val sampler = new Sampler(bootstrap)

  def makeDisseminator = {
    val config = Config(sampler, 3, 6, BloomFilter[String](1000, .001) )
    new Disseminator(config)

  }

  val lhost = "127.0.0.1"

  def nextPeer = Addr.make(lhost, RandomSocket.nextPort)
  val peers = List.fill(10)(nextPeer)

  val views = peers.map {p =>
    PeerView(p)
  }

  val bootstrap_peer = peers.head 
  val servers = views.map {x => new Server(x, makeDisseminator) }

  def makeListener = {
    new Listener[RMSG, TMSG](Mux.Pipelines.serverPipeline, pool, classOf[NioServerSocketChannel] )
  }



  val bs :: rest = servers 

  val boot = bs.start(makeListener)

  boot onSuccess { x =>

    rest map { srv =>
      srv.bootstrap(bootstrap_peer, makeListener)
    }
  }



  def check_views(): Future[Unit] = Future {
    Thread.sleep(1000)
    servers.foreach {x =>
      val n = x.view.neighbors.size
      val addr = Addr.toString(x.view.local)
      val info = s"$addr has $n neighbors"
      println(info)
    }
    check_views
  }

  check_views
  Await.result(boot)


}



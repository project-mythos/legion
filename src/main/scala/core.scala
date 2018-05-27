package legion.core

import io.netty.channel.{ ChannelInitializer , Channel}
import io.netty.channel.socket.SocketChannel

import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.bootstrap.Bootstrap
import java.net.InetSocketAddress
import scala.util.Random


import gossip.{Peer, PeerView}
import enkidu.mux_proto._
import com.twitter.util.{Future}

import enkidu.{Connection, WorkerPool, ChannelFlow, Flow}


object Client {
  def init() = new ChannelInitializer[Channel] {

    def initChannel(ch: Channel) = {
      ch.pipeline.addLast(new TMSGEncoder())
      ch.pipeline.addLast(new RMSGDecoder())
    }

  }


  def peerToAddr(peer: Peer) = {
    new InetSocketAddress( peer.host, peer.port)
  }

  def connect(bootstrap: Bootstrap, peer: Peer): Future[ Flow[TMSG, RMSG] ]= {

    val addr = peerToAddr(peer)
    val makeFlow = {ch: Channel => new ChannelFlow(ch) }

    Connection.fromBootstrap[TMSG, RMSG]( addr, bootstrap, makeFlow) 
  }


  def bootstrap(pool: WorkerPool) = {
    val b = new Bootstrap()

    b
      .group(pool.group)
      .channel(classOf[NioSocketChannel])
      .handler(init())

    b
  }



  def dispatch(trans: Flow[TMSG, RMSG], req: TMSG): Future[RMSG] = {
    trans.write(req) flatMap { x => trans.read() }
  }


  def fireForget(conn: Flow[TMSG, RMSG], msg: TMSG) = {
    conn.write(msg) ensure {conn.close()} 
  }

}





trait PeerService {
  def connect(peer: Peer): Future[Flow[TMSG, RMSG] ]

  def view: PeerView
  def local: Peer 

  def join(peer: Peer): Unit
  def dead(peer: Peer): Unit

  def sample(fanout: Int): Future[ Seq[ (Peer, Flow[TMSG, RMSG]) ] ] 
  def sampleOne(): Future[(Peer, Flow[TMSG, RMSG])  ]

}




class DefaultPeerService(seed: PeerView, bs: Bootstrap) extends PeerService {

  private var peerView  = seed
  def connect(peer: Peer) = Client.connect(bs, peer) 

  def view = peerView
  def local = peerView.local

  def join(peer: Peer) = {
    val pv2 = view.copy(neighbors = view.neighbors :+ peer)
    synchronized {peerView = pv2} 
  }

  def dead(peer: Peer) = {
    val pv2 = view.copy(neighbors = view.neighbors.filter(x => x != peer) )
    synchronized {peerView = pv2} 
  }


  def select(f: Int) = {
    Random.shuffle(view.neighbors).take(f) 
  }



  //plan for failure here to close remaining channels if N connections failed to be established
  def sample(f: Int): Future[Seq[(Peer, Flow[TMSG,RMSG]) ] ]  = {
    val sample = select(f)
    val connsF = sample.map {peer =>
      Client.connect(bs, peer) map {trans => (peer, trans)} 
    }

    Future.collect(connsF) 
  }


  def sampleOne(): Future[ (Peer, Flow[TMSG, RMSG]) ]= {
    val host = select(1).head
    Client.connect(bs, host) map {trans => (host, trans)} 
  }


}


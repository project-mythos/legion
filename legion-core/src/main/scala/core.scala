package legion

import io.netty.channel.{ ChannelInitializer , Channel}
import io.netty.channel.socket.SocketChannel

import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.bootstrap.Bootstrap
import java.net.InetSocketAddress
import scala.util.Random


import gossip.{Peer, PeerView}
import enkidu.mux._
import com.twitter.util.{Future, Promise}

import enkidu.{Connection, WorkerPool, ChannelFlow, Flow}

trait PeerService {
  def connect(peer: Peer): Future[Flow[TMSG, RMSG] ]

  def view: PeerView
  def local: Peer 

  def join(peer: Peer): Unit
  def dead(peer: Peer): Unit

  def select(fanout: Int): List[Peer]
  def selectOne(): Peer
}

object PeerConnection {

  def toAddress(peer: Peer) = {
    new InetSocketAddress(peer.host, peer.port)
  }

  def connect(bs: Bootstrap, peer: Peer) = {
    Connection.connect[TMSG, RMSG](bs, toAddress(peer) )
  }
}




class DefaultPeerService(seed: PeerView, bs: Bootstrap) extends PeerService {

  private var peerView  = seed

  def connect(peer: Peer) = PeerConnection.connect(bs, peer)

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
    Random.shuffle(view.neighbors).take(f).toList
  }



  //plan for failure here to close remaining channels if N connections failed to be established


  def selectOne(): Peer = {
    select(1).head
  }


}


object DefaultPeerService {

  def initMux() = new ChannelInitializer[Channel] {

    def initChannel(ch: Channel) = {
      ch.pipeline.addLast(new TMSGEncoder())
      ch.pipeline.addLast(new RMSGDecoder())
    }

  }

  def make(pool: WorkerPool, seed: PeerView) = {
    val b = Connection.bootstrap[TMSG, RMSG](classOf[NioSocketChannel], initMux, pool)
    new DefaultPeerService(seed, b)
  }
}

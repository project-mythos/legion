package Legion

import io.netty.channel.{ ChannelInitializer , Channel}
import io.netty.channel.socket.SocketChannel

import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.bootstrap.Bootstrap
import java.net.InetSocketAddress
import scala.util.Random


import Gossip.Messages.Peer
import Enkidu.Mux._
import com.twitter.util.{Future, Promise}

import Enkidu.{Connection, WorkerPool, ChannelFlow, Flow}
import Enki_DT.{SetCRDT, ORSet}




case class PeerView(local: Peer, membership: ORSet[Peer]) {

  def neighbors = {
    (ORSet.query(membership) - local).toList
  }

  def commit(m: ORSet[Peer]) = {
    this.copy(membership = m)
  }

  def join(peer: Peer) = {
    val ns = ORSet.add(membership, peer)
    commit(ns)
  }


  def leave(peer: Peer) = {
    val ns = ORSet.delete(membership, peer)
    commit(ns)
  }


}



object PeerView {
  def apply(local: Peer): PeerView = {
    val mlist = ORSet.empty(local.id) |> {os => ORSet.add(os, local) }
    PeerView(local, mlist)
  }
}



object PeerConnection {

  def toAddress(peer: Peer) = {
    new InetSocketAddress(peer.host, peer.port)
  }

  def connect(bs: Bootstrap, peer: Peer) = {
    Connection.connect[TMSG, RMSG](bs, toAddress(peer) )
  }
}



class Sampler(BS: Bootstrap) {


  def connect(peer: Peer) = PeerConnection.connect(BS, peer)

  def select(view: PeerView, f: Int) = {
    Random.shuffle(view.neighbors).take(f).toList
  }

  def selectOne(view: PeerView): Peer = {
    val i = Random.nextInt(view.neighbors.size)
    view.neighbors(i)
  }


  def select(view: PeerView, f: Int, pred: Peer => Boolean): List[Peer] = {
    val n1 = view.neighbors.filter(pred)
    Random.shuffle(n1).take(f).toList
  }


  def selectOne(view: PeerView, pred: Peer => Boolean ) = {
    val n1 = view.neighbors.filter(pred)
    val i = Random.nextInt(n1.size)
    n1(i)
  }


}



/**
object DefaultPeerService {

  def initMux() = new ChannelInitializer[Channel] {

    def initChannel(ch: Channel) = {
      ch.pipeline.addLast(new TMSGEncoder())
      ch.pipeline.addLast(new RMSGDecoder())
    }

  }

  def make(pool: WorkerPool, view: PeerView) = {
    val b = Connection.bootstrap[TMSG, RMSG](classOf[NioSocketChannel], initMux, pool)
    new DefaultPeerService(b)
  }


}
  
**/
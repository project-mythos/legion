package Legion
import Enki.{SyncVar, Pool}

import Enkidu.ConnectionManager
import java.net.InetSocketAddress
import scala.util.Random
import Enkidu.Mux._
import Enkidu.Node

import Gossip.Messages._

import com.twitter.util.{Future, Promise, Return, Throw}
import Enkidu.{Connection, WorkerPool, ChannelFlow, Flow}
import scala.collection.concurrent.TrieMap

import com.twitter.util.{Future, Promise}


/** Samplers are in charge of connection management and peer selection */
trait Sampler {

  def connect[T](peer: Node)(f: Flow[TMSG, RMSG] => Future[T]): Future[T]
  def select(view: View, i: Int): List[Node]
  def selectOne(view: View): Node

  def select(view: View, f: Int, p: Node => Boolean): List[Node]
  def selectOne(view: View, p: Node => Boolean): Node
}



/** Simple Selection mixin that picks nodes via scala.util.Random */
trait RandomSelection {
  def select(view: View, f: Int) = {
    Random.shuffle(view.neighbors).take(f).toList
  }

  def selectOne(view: View): Node = {
    val i = Random.nextInt(view.neighbors.size)
    view.neighbors(i)
  }


  def select(view: View, f: Int, pred: Node => Boolean): List[Node] = {
    val n1 = view.neighbors.filter(pred)
    Random.shuffle(n1).take(f).toList
  }


  def selectOne(view: View, pred: Node => Boolean ) = {
    val n1 = view.neighbors.filter(pred)
    val i = Random.nextInt(n1.size)
    n1(i)
  }
}



/** A sampler that uses the RandomSelection mixin and closes the connection after you are done with it*/
class SimpleSampler(CM: ConnectionManager[TMSG, RMSG]) extends Sampler with RandomSelection {


  def connect[T](peer: Node)(f: Flow[TMSG, RMSG] => Future[T])  = {
    CM.connect(peer)(f)
  }

}
 

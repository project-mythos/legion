package Legion
import org.scalatest._

import Enkidu._
import Enkidu.Mux._
import Gossip.Messages._
import com.twitter.util.{ Future, Await, RandomSocket}
import io.netty.channel.embedded.EmbeddedChannel

object Helpers {
  val lhost = "localhost"
  def nextPeer = Addr.make(lhost, RandomSocket.nextPort)
  def isSome[T](o: Option[T]) = o match {
    case Some(x) => true
    case _ => false
  }
}




object EmbeddedUtil {

  def write(ch: EmbeddedChannel, t: Object) = {
    ch.writeOutbound(t)
    ch.writeInbound(ch.readOutbound)
  }

  def read[T](ch: EmbeddedChannel) = {
    ch.readInbound().asInstanceOf[T]
  }
}


class SessionProtoTests extends FunSuite {

  val TChan = new EmbeddedChannel( new TMSGEncoder(), new TMSGDecoder() )
  val RS = new RumorSession(TMSG)
  val ES = new ExchangeSession(TMSG)

  val treq = TMSG(List("hello") )


  val rumor = Rumor(Helpers.nextPeer)
  val treq1 = RS.encode(treq, rumor)

  test("Rumor should be embedded in request") {
    val got = RS.decode(treq1)
    assert(Helpers.isSome(got))
    assert(rumor == got.get) 
  }




  test("Should play well with netty TMSG codec") {
    EmbeddedUtil.write(TChan, treq1)
    val got = EmbeddedUtil.read[TMSG](TChan)
    val result = RS.decode(got)

    println(got.headers)

    assert(Helpers.isSome(result) )
  }

}

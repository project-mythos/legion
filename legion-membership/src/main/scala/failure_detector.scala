package legion.membership

import Enkidu.{Connection}
import Enkidu.Mux.{TMSG, RMSG, Headers}

import legion.rumor_proto._
import gossip._

import com.twitter.util.{Future, Duration, JavaTimer}
import Disseminator.ClientFlow
import java.util.UUID

object MembershipClient {

  def suspect(trans: ClientFlow, suspected: Peer): Future[Reply] = {
    val tmsg = TMSG(Paths.suspect, Headers.empty, suspected.toByteArray)

    Connection.send_recv(trans, tmsg) map { rep =>
      Reply.parseFrom(rep.payload)
    } ensure {trans.close()}


  }


  def ping(trans: ClientFlow) = {
    val p = Ping().toByteArray
    val tmsg = TMSG(Paths.ping, Headers.empty, p)
    Connection.send_recv(trans, tmsg) ensure {trans.close();}
  }



  def join(trans: ClientFlow, from: Peer) = {
    val tmsg = TMSG(Paths.join, Headers.empty, from.toByteArray)

    Connection.send_recv(trans,tmsg) map { rep  =>
      PeerView.parseFrom(rep.payload).copy(local=from)
    } ensure {trans.close();}

  }

}



class FailureDetector(config: Config, interval: Duration) {

  val Disseminator = new Disseminator(config)

  val PS = config.PS
  val fanout = config.fanout


  def broadcastDeath(deadPeer: Peer): Unit = {
    PS.dead(deadPeer)

    val change = deadPeer.toByteString
    val rumor = Disseminator.newRumor(change)


    def toTMSG(r: Rumor) = {
      val path = Paths.death_rumor
      TMSG(path, Headers.empty, rumor.toByteArray)
    }

    Disseminator.spread(rumor, toTMSG)
  }



  def suspectPeer(deadPeer: Peer): Future[Unit] = {

    val rid = UUID.randomUUID().toString

    val reportTo = PS.selectOne()

    val f = PS.connect(reportTo) flatMap {flow =>
      MembershipClient.suspect(flow, deadPeer)
    }

    f onFailure(e => suspectPeer(deadPeer) )
   

    f map {rep =>
      if (rep.success != true) { broadcastDeath(deadPeer) }
    } 

    
  }

  def detect(peer: Peer) = {

    PS.connect(peer) flatMap {flow =>
      MembershipClient.ping(flow)
    } rescue {case e => suspectPeer(peer) }  

  }


  def detector() = {
    PS.select(config.fanout) map { peer =>
      detect(peer)
    }
  }


  val timer = new JavaTimer()
  timer.schedule(interval)(detector)

}



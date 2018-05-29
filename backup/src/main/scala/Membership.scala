package legion.membership


import com.twitter.util.{Promise, Future, Duration, JavaTimer, Return, Throw }
import legion.core.{Client, PeerService}

import enkidu.{Flow}
import enkidu.mux_proto.{TMSG, RMSG, Headers}

import legion.rumor_proto._
import gossip._
import java.util.UUID
import com.google.protobuf.ByteString

import legion.operators.PipeOps._
import Disseminator.{ ServerFlow, ClientFlow }


object MembershipClient {

  def suspect(trans: ClientFlow, suspected: Peer): Future[Reply] = {
    val tmsg = TMSG(Paths.suspect, Headers.empty, suspected.toByteArray)

    Client.dispatch(trans, tmsg) map { rep =>
      Reply.parseFrom(rep.payload)
    } ensure {trans.close()}


  }


  def ping(trans: ClientFlow) = {
    val p = Ping().toByteArray
    val tmsg = TMSG(Paths.ping, Headers.empty, p)
    Client.dispatch(trans, tmsg) ensure {trans.close();}
  }



  def join(trans: ClientFlow, from: Peer) = {
    val tmsg = TMSG(Paths.join, Headers.empty, from.toByteArray)

    Client.dispatch(trans,tmsg) map { rep  =>
      PeerView.parseFrom(rep.payload).copy(local=from)
    } ensure {trans.close();}

  }


}



class FailureDetector(config: Config, interval: Duration) {

  val sampler = config.sampler
  val fanout = config.fanout


  def broadcastDeath(deadPeer: Peer): Unit = {
    sampler.dead(deadPeer)

    val change = deadPeer.toByteString
    val rumor = Disseminator.newRumor(config, change)


    def toTMSG(r: Rumor) = {
      val path = Paths.death_rumor
      TMSG(path, Headers.empty, rumor.toByteArray)
    }

    Disseminator.spread(config, rumor, toTMSG)
  }



  def suspectPeer(deadPeer: Peer): Future[Unit] = {

    val rid = UUID.randomUUID().toString

    val reportTo = sampler.selectOne()

    val f = sampler.connect(reportTo) flatMap {flow =>
      MembershipClient.suspect(flow, deadPeer)
    }

    f onFailure(e => suspectPeer(deadPeer) )
   

    f map {rep =>
      if (rep.success != true) { broadcastDeath(deadPeer) }
    } 

    
  }

  def detect(peer: Peer) = {

    sampler.connect(peer) flatMap {flow =>
      MembershipClient.ping(flow)
    } rescue {case e => suspectPeer(peer) }  

  }


  def detector() = {
    sampler.select(config.fanout) map { peer =>
      detect(peer)
    }
  }


  val timer = new JavaTimer()
  timer.schedule(interval)(detector)

}






object Paths {

  val join = List("membership", "join")
  val suspect = List("membership", "suspect")
  val ping = List("membership", "ping")

  val death_rumor = List("membership", "rumors", "dead")
  val join_rumor = List("membership", "rumors", "join")


}



object Handlers { 


  def replyOK(flow: ServerFlow, body: ByteString) = {

    val reply= Reply(true, body)
    val rmsg = RMSG(Headers.empty, reply.toByteArray)
    flow.write(rmsg) ensure { flow.close(); }
  }


  def emptyReply(success: Boolean): Reply = {
    val body = ByteString.copyFrom(Array[Byte]())
    Reply(success, body)
  }


  def replyToRMSG(reply: Reply): RMSG = {
    RMSG(Headers.empty, reply.toByteArray)
  }


  def join(config: Config, peer: Peer, flow: ServerFlow) = {
    config.sampler.join(peer)
    val payload = config.sampler.view.toByteString
    replyOK(flow, payload)
  }


  def joinRumor(config: Config, rumor: Rumor) = {


    
    def processRumor(r: Rumor) = {
      val peer = r.payload.toByteArray() |> Peer.parseFrom

      config.sampler.join(peer)
    }


    def toTMSG(r: Rumor) = TMSG(Paths.join_rumor, Headers.empty, r.toByteArray)

    Disseminator.handleRumor(config, rumor, toTMSG, processRumor)

  }



  def deadRumor(config: Config, rumor: Rumor) = {

    def processRumor(r: Rumor) = {
      val peer = r.payload.toByteArray() |> Peer.parseFrom
      config.sampler.dead(peer)
    }


    def toTMSG(r: Rumor) = TMSG(Paths.death_rumor, Headers.empty, r.toByteArray)

    Disseminator.handleRumor(config, rumor, toTMSG, processRumor)
  }




  def suspect(config: Config, peer: Peer, flow: ServerFlow) = {

    val sock = config.sampler.connect(peer)


    val f = sock flatMap { trans =>
      MembershipClient.ping(trans) map {x => emptyReply(true) |> replyToRMSG }
    }


    def isDead() = {
      config.sampler.dead(peer)
      Future { emptyReply(false) |> replyToRMSG}
    }

    val f1 = f rescue { case _ => isDead() }
    f1 flatMap { rep => flow.write(rep) } 

  }


  def handle(config: Config, flow: ServerFlow) = {
    val reqF = flow.read()

    reqF flatMap { req =>

    req.path match {
      case Paths.join_rumor =>
        val rumor = Rumor.parseFrom(req.payload)
        joinRumor(config, rumor) 


      case Paths.death_rumor =>
        val rumor = Rumor.parseFrom(req.payload)
        deadRumor(config, rumor)

      case Paths.suspect =>
        val peer = Peer.parseFrom(req.payload)
        suspect(config, peer, flow) 


      case Paths.join =>
        val peer = Peer.parseFrom(req.payload)
        join(config, peer, flow) 


      case Paths.ping =>
        val body = "PONG".getBytes("UTF-8")
        RMSG(Headers.empty, body) |> flow.write  
        

      case _ =>
        val body = "Error No Such Resource Exists".getBytes("UTF-8") 
        RMSG(Headers.empty, body) |> flow.write  
    }
  } }


}

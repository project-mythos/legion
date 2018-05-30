package legion.membership


import com.twitter.util.Future
import legion.PeerService
import Enkidu.Mux.{TMSG, RMSG, Headers}

import legion.rumor_proto._
import gossip._
import com.google.protobuf.ByteString

import legion.operators.PipeOps._
import Disseminator.ServerFlow


object Paths {

  val join = List("membership", "join")
  val suspect = List("membership", "suspect")
  val ping = List("membership", "ping")

  val death_rumor = List("membership", "rumors", "dead")
  val join_rumor = List("membership", "rumors", "join")


}



class Handlers(config: Config) {



  val Disseminator = new Disseminator(config) 
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


  def join(peer: Peer, flow: ServerFlow) = {
    config.PS.join(peer)
    val payload = config.PS.view.toByteString
    replyOK(flow, payload)
  }


  def joinRumor(rumor: Rumor) = {


    
    def processRumor(r: Rumor) = {
      val peer = r.payload.toByteArray() |> Peer.parseFrom

      config.PS.join(peer)
    }


    def toTMSG(r: Rumor) = TMSG(Paths.join_rumor, Headers.empty, r.toByteArray)

    Disseminator.handleRumor( rumor, toTMSG, processRumor)

  }



  def deadRumor(rumor: Rumor) = {

    def processRumor(r: Rumor) = {
      val peer = r.payload.toByteArray() |> Peer.parseFrom
      config.PS.dead(peer)
    }


    def toTMSG(r: Rumor) = TMSG(Paths.death_rumor, Headers.empty, r.toByteArray)

    Disseminator.handleRumor(rumor, toTMSG, processRumor)
  }




  def suspect(peer: Peer, flow: ServerFlow) = {

    val sock = config.PS.connect(peer)


    val f = sock flatMap { trans =>
      MembershipClient.ping(trans) map {x => emptyReply(true) |> replyToRMSG }
    }


    def isDead() = {
      config.PS.dead(peer)
      Future { emptyReply(false) |> replyToRMSG}
    }

    val f1 = f rescue { case _ => isDead() }
    f1 flatMap { rep => flow.write(rep) } 

  }


  def handle(req: TMSG, flow: ServerFlow) = {
 
    req.path match {
      case Paths.join_rumor =>
        val rumor = Rumor.parseFrom(req.payload)
        joinRumor(rumor) 


      case Paths.death_rumor =>
        val rumor = Rumor.parseFrom(req.payload)
        deadRumor(rumor)

      case Paths.suspect =>
        val peer = Peer.parseFrom(req.payload)
        suspect(peer, flow) 


      case Paths.join =>
        val peer = Peer.parseFrom(req.payload)
        join( peer, flow) 


      case Paths.ping =>
        val body = "PONG".getBytes("UTF-8")
        RMSG(Headers.empty, body) |> flow.write  
        

      case _ =>
        val body = "Error No Such Resource Exists".getBytes("UTF-8") 
        RMSG(Headers.empty, body) |> flow.write  


    }
  }

}

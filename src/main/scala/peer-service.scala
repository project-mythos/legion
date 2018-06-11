package Legion.PeerService

import Legion.Epidemic.Disseminator
import Legion.AntiEntropy.{Sync, Synchronizer}

import Legion._
import com.twitter.util.{Promise, Future, Duration, JavaTimer}
import Gossip.Messages._, Enki_DT.Proto.{ORSetBridge, PBBridge, DTCodec}

import Enki_DT.ORSet
import Enkidu.Mux._
import Enkidu._
import Enki.PipeOps._ 

//Decide whether I need to close connections or pool them


object PeerCodec extends DTCodec[Peer] {
  def encode(p: Peer) = p.toByteArray
  def decode(bytes: Array[Byte]) = Peer.parseFrom(bytes)
}

object PeerViewSync extends PBBridge[PeerView, PV] with Sync[PeerView] {

  val companion = PV
  val OSB = new ORSetBridge(PeerCodec)

  def fromPB(pv: PV) = {
    val members = OSB.fromPB( pv.mlist )
    val local = pv.local
    PeerView(local, members)
  }


  def toPB(view: PeerView) = {
    val mlist = OSB.toPB(view.membership)
    val local = view.local
    PV(local, mlist)
  }


  def merge(l: PeerView, r: PeerView) = {
    val m1 = ORSet.merge(l.membership, r.membership)
    l.copy(membership=m1)
  }


}




/** Most of these functions initiate rumor propagation */
class Client(D: Disseminator)  {

  val PS = D.conf.PS 

  val RmrTS = D.RumorSession
  val VSync = new Synchronizer(PeerViewSync, PS)

  val ExchTS = VSync.TS

  
  def join(local: Peer, bs: Peer): Future[PeerView] = {
    val path = List("memberlist", "join")
    val rumor = Rumor.zero(local)

    D.markSeen(rumor)

    val req = TMSG(path, local.toByteArray) |> { msg => RmrTS.encode(msg, rumor)

    }

    val rep = PS.connect(bs) flatMap {flow =>
      Connection.send_recv(flow, req)
    }

    rep map {msg =>
      val seed = PeerViewSync.decode(msg.payload)
      val view = PeerViewSync.merge( PeerView(local), seed )
      view
    }

  }


  def leave(view: PeerView): Future[Unit] = {
    val path = List("memberlist", "leave")
    val rumor = Rumor(view.local)

    val req = TMSG(path, view.local.toByteArray)
    D.spread(view, rumor, req)

  }



  def ping(peer: Peer) = {

    val p = new Promise[Boolean]()

    val req = TMSG(List("memberlist", "ping"), peer.toByteArray)

    val acked = PS.connect(peer) flatMap {
      flow => Connection.send_recv(flow, req)
    } map {reply => pingAcked(reply) } 

    acked onSuccess { x => 
      p.become(acked)
    }

    acked onFailure { e =>
      p.setValue(false)
    }

    p 
  }



  def pingAcked(rep: RMSG) = {
    val text = new String(rep.payload)

    text match {
      case "PONG" => true
      case _ => false 
    }

  }


  //maybe make it have a dead peer list 
  def indirect_ping(view: PeerView, suspect: Peer): Future[PeerView] = {

    def p(peer: Peer) = peer != suspect

    val host: Peer = PS.selectOne(view, p)
 
    val flow = PS.connect(suspect)
    val path = List("memberlist", "ping", "indirect")

    val rumor = Rumor.zero(view.local)

    D.markSeen(rumor)

    val body = suspect.toByteArray
    val msg = TMSG(path, body) |> {m => RmrTS.encode(m, rumor) }


    val acked = flow flatMap { f =>
      Connection.send_recv(f, msg)
    } map {rep => pingAcked(rep) } 

    acked map {b =>
      if (b) view
      else view.leave(suspect) 
    } onFailure {e =>

      indirect_ping(view, host)
      indirect_ping(view, suspect)
    }


  }



  def dead_suspect(view: PeerView, rumor: Rumor, peer: Peer): Future[PeerView] = {
    val path = List("memberlist", "dead")


    val req = TMSG(path, peer.toByteArray)
    D.forward(view, rumor, req) map { f =>
      view.leave(peer)
    }
  }


  def view_memberlist(peer: Peer) = {
    val path = List("memberlist", "view")
    val req = TMSG(path) 
    val conn = PS.connect(peer)

    conn flatMap { flow => Connection.send_recv(flow, req) } map {rep =>
      PeerViewSync.decode(rep.payload)
    }

  }


  def exchange_views(view: PeerView): Future[PeerView] = {
    val path = List("memberlist", "exchange")
    val msg = TMSG(path)

    VSync.tsync(view, view, msg)
  } 



}




import Enki.SyncVar


class Server(seed: PeerView, D: Disseminator) {

  val state = new SyncVar(seed)
  val RumorP = D.RumorSession
  val PS = D.conf.PS

  val CLI = new Client(D)

  val ExchP = CLI.ExchTS
  val VSync = CLI.VSync

  val pong = RMSG( "PONG".getBytes("utf8") )

  def view: PeerView = state.get


  def isSource(rumor: Rumor, peer: Peer) = {
    (rumor.from == peer) &&  (rumor.round == 0)  
  }


  

  def join(flow: SFlow, tmsg: TMSG) = {

    def op(rumor: Rumor, tmsg: TMSG) = {

      val peer = Peer.parseFrom(tmsg.payload)
      val isBS = isSource(rumor, peer)

      val v1 = state.update(v => v.join(peer)) 
      (isBS, v1)

      if (isBS) {
        val rep = RMSG( PeerViewSync.encode(v1) ) 
        flow.write(rep)
      } else {
        Future.Done
      }
    }

    D.handleRumor(view, tmsg, op)
  }


  def leave(tmsg: TMSG) = {

    val peer = Peer.parseFrom(tmsg.payload)
    val v1 = state.update(v => v.leave(peer) )
    Future.Done 
  }


  def indirect_ping(flow: SFlow, tmsg: TMSG, rumor: Rumor) = {
    val peer = Peer.parseFrom(tmsg.payload)

    val repF = CLI.ping(peer) map {b =>
      if (true) pong
      else RMSG.empty()
    }

    CLI.dead_suspect(view, rumor, peer)

    repF 
  }


  def dead(tmsg: TMSG): Future[Unit]= leave(tmsg)

  def ping(tmsg: TMSG): Future[RMSG] = Future.value(pong)

  def view_memberlist: Future[RMSG] = {
    val payload = view |> PeerViewSync.encode
    Future.value( RMSG(payload) )
  }

  def exchange_views(flow: Flow[RMSG, TMSG], tmsg: TMSG, exch: Exchange) = {
    VSync.rsync(view, flow)(exch, view, tmsg) map {v1 => state.put(v1) }
  }



  type SFlow = Flow[RMSG, TMSG]




  def memberlist_cb(flow: Flow[RMSG, TMSG], req: TMSG) = {
    

     def leave_op(rumor: Rumor, tmsg: TMSG) = { leave(tmsg) } 
       

    req.path match {

      case List("memberlist", "join") => join(flow, req)

      case List("memberlist", "leave") =>  D.handleRumor(view, req, leave_op)

      case List("memberlist", "ping") => ping(req) flatMap {rep => flow.write(rep)}

      case List("memberlist", "view") => view_memberlist flatMap {rep => flow.write(rep) }

      case List("memberlist", "ping", "indirect") =>
        indirect_ping(flow, req, RumorP.decode(req).get) flatMap(rep => flow.write(rep) )

      case List("memberlist", "dead") =>
        D.handleRumor(view, req, leave_op)

      case List("memberlist", "exchange") =>
        exchange_views(flow, req, ExchP.decode(req).get )

      case _ => Future.Done
    }

  }


 

  type CB = (Flow[RMSG, TMSG], TMSG) => Future[Unit]


  def handler(callback: CB)(flow: Flow[RMSG, TMSG]) = {
    flow.read() flatMap {req => callback(flow, req)  }
  }

  def withMemberListCB(cb: CB)(flow: Flow[RMSG, TMSG], req: TMSG) = {

    req.path match {
      case "memberlist" :: tl => memberlist_cb(flow, req)
      case _ => cb(flow, req)
    }

  }



  
  def start(
    L: Listener[RMSG, TMSG],
    fdInterval: Duration = Procs.defaultInterval,
    syncInterval: Duration = Procs.defaultInterval,  
    cb: Option[CB] = None
  ) = {

    val laddr = Addr.toSocketAddress(view.local)

    cb match {
      case Some(callback) =>
        val cb1: CB = withMemberListCB(callback)
        L.listen(laddr)( handler(cb1) )

      case None =>
        L.listen(laddr)( handler(memberlist_cb) )
    }

    Procs.failureDetector(fdInterval)
    Procs.view_maintenance(fdInterval)
  }

  def bootstrap(
    contact: Peer, 
    L: Listener[RMSG, TMSG],
    fdInterval: Duration = Procs.defaultInterval,
    syncInterval: Duration = Procs.defaultInterval,
    cb: Option[CB] = None
  ) = {
    
  }




  object Procs {
   

    import com.twitter.conversions.time._

    val defaultInterval = 100 milliseconds

    val timer = new JavaTimer()

    def failureDetector(interval: Duration) = {


      def detect = {
        if (notEmpty)  {

          PS.select(view, D.conf.fanout) foreach { peer =>
            CLI.ping(peer) flatMap {x =>
              if (x) Future.Done
              else CLI.indirect_ping(view, peer)
            }
          }

        }
      }




      timer.schedule(interval)(detect)  
    }

    def notEmpty = view.neighbors.isEmpty != true



    def view_maintenance(interval: Duration) = {

      def exch = if (notEmpty) CLI.exchange_views(view) map {x => state.put(x) }

      timer.schedule(interval)(exch)
    }
  }


}


object Server {





}
package Legion.PeerService

import Legion.Epidemic.Disseminator
import Legion.AntiEntropy.{Sync, Contents}

import Legion._
import com.twitter.util.{Promise, Future, Duration, JavaTimer}
import Gossip.Messages._, Enki_DT.Proto.{ORSetBridge, PBBridge, DTCodec}

import Enki_DT.ORSet
import Enkidu.Mux._
import Enkidu._
import Enki.PipeOps._ 

//Decide whether I need to close connections or pool them



object ViewSync extends PBBridge[View, PV] with Contents[View] {

  val companion = PV
  val OSB = new ORSetBridge(Node2)

  def fromPB(pv: PV) = {
    val members = OSB.fromPB( pv.mlist )
    val local = Node2.fromPB( pv.local )
    View(local, members)
  }


  def toPB(view: View) = {
    val mlist = OSB.toPB(view.membership)
    val local = Node2.toPB( view.local )
    PV(local, mlist)
  }


  def merge(l: View, r: View) = {
    val m1 = ORSet.merge(l.membership, r.membership)
    l.copy(membership=m1)
  }


}




/** Most of these functions initiate rumor propagation */
class Client(D: Disseminator)  {

  import Node2._
  val PS = D.conf.PS 

  val RmrTS = D.RumorSession
  val VSync = new Sync(ViewSync, PS)



  val ExchTS = VSync.TS

  
  def join(local: Node, bs: Node): Future[View] = {
    val path = List("memberlist", "join")
    val rumor = Rumor.zero(local)

    D.markSeen(rumor)

    val req = TMSG(path, local.toByteArray) |> { msg => RmrTS.encode(msg, rumor) }

    val rep = PS.connect(bs) {flow =>
      Connection.send_recv(flow, req)
    }

    rep map {msg =>
      val seed = ViewSync.decode(msg.payload)
      val view = ViewSync.merge( View(local), seed )
      view
    }

  }


  def leave(view: View): Future[Unit] = {
    val path = List("memberlist", "leave")
    val rumor = Rumor(view.local)

    val req = TMSG(path, view.local.toByteArray)
    D.spread(view, rumor, req)

  }



  def ping(peer: Node) = {

    val p = new Promise[Boolean]()

    val req = TMSG(List("memberlist", "ping"), peer.toByteArray)

    val acked = PS.connect(peer) { flow =>
      Connection.send_recv(flow, req)
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
  def indirect_ping(view: View, suspect: Node): Future[View] = {

    def p(peer: Node) = peer != suspect

    val host: Node = PS.selectOne(view, p)

    val path = List("memberlist", "ping", "indirect")

    val rumor = Rumor.zero(view.local)

    D.markSeen(rumor)

    val body = suspect.toByteArray
    val msg = TMSG(path, body) |> {m => RmrTS.encode(m, rumor) }


    val acked = PS.connect(host) { f =>
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



  def dead_suspect(view: View, rumor: Rumor, peer: Node): Future[View] = {
    val path = List("memberlist", "dead")
    val req = TMSG(path, peer.toByteArray)

    D.forward(view, rumor, req) map { f =>
      view.leave(peer)
    }
  }


  def view_memberlist(peer: Node) = {
    val path = List("memberlist", "view")
    val req = TMSG(path) 
    

   PS.connect(peer) { flow => Connection.send_recv(flow, req) } map {rep =>
      ViewSync.decode(rep.payload)
    }

  }


  def exchange_views(view: View): Future[View] = {
    val path = List("memberlist", "exchange")
    val msg = TMSG(path)

    VSync.tsync(view, view, msg)
  } 



}





import Enki.SyncVar


class Server(seed: View, val D: Disseminator) {

  val state = new SyncVar(seed)

  val PS = D.conf.PS

  val CLI = new Client(D)

  val VSync = CLI.VSync



  val pong = RMSG( "PONG".getBytes("utf8") )
  val Rumor_Filters = new Epidemic.Filters(view, D)


  def view(): View = state.get


 
  def join(tmsg: TMSG) = {
    val peer = Node2.decode(tmsg.payload)
    val v1 = state.update(v => v.join(peer))    
    Future{ RMSG( ViewSync.encode(v1) ) }
  }


  def leave(tmsg: TMSG) = {

    val peer = Node2.decode(tmsg.payload)
    val v1 = state.update(v => v.leave(peer) )
    Future.Done 
  }


  def indirect_ping(tmsg: TMSG, rumor: Rumor) = {
    val peer = Node2.decode(tmsg.payload)

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
    val payload = view |> ViewSync.encode
    Future.value( RMSG(payload) )
  }

  def exchange_views(flow: Flow[RMSG, TMSG], tmsg: TMSG) = {
    VSync.rsync(flow, view, tmsg) map {v1 => state.put(v1) }
  }



  type SFlow = Flow[RMSG, TMSG]

  def memberlist_cb(flow: Flow[RMSG, TMSG], req: TMSG): Future[Unit] = {
   


    req.path match {

      case List("memberlist", "join") =>
        Rumor_Filters.needs_rumor(flow, req) { case (req1, rumor) =>
          Rumor_Filters.first_responder(flow, req1, rumor)(join) 
        }
       

      case List("memberlist", "leave") =>  Rumor_Filters.rumor_filter(req)(leave)

      case List("memberlist", "ping") => ping(req) flatMap {rep => flow.write(rep)}

      case List("memberlist", "view") => view_memberlist flatMap {rep => flow.write(rep) }

      case List("memberlist", "ping", "indirect") =>

        Rumor_Filters.needs_rumor(flow, req){ case (req1, rumor) =>
          indirect_ping(req1, rumor) flatMap (rep => flow.write(rep) )
        }

      case List("memberlist", "dead") =>
        Rumor_Filters.rumor_filter(req)(dead)

      case List("memberlist", "exchange") =>
        exchange_views(flow, req)

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
    cb: CB
  ) = {

    val laddr = Node.toSocketAddress(view.local)

    val cb1: CB = withMemberListCB(cb)
    val l = L.listen(laddr)( handler(cb1) )


    l map { x =>

      Procs.failureDetector(fdInterval)
      Procs.view_maintenance(fdInterval)
      L
    }
  }



  def bootstrap(
    contact: Node,
    L: Listener[RMSG, TMSG],
    fdInterval: Duration = Procs.defaultInterval,
    syncInterval: Duration = Procs.defaultInterval,
    cb: CB
  ) = {


    start(L, fdInterval, syncInterval, cb) flatMap { x =>

      CLI.join(view.local, contact) map { x =>
        state.update{v => ViewSync.merge(v, x) }
      }

    }
  }




  object Procs {

    import com.twitter.conversions.time._

    val defaultInterval = 100 milliseconds

    val timer = new JavaTimer()

    def failureDetector(interval: Duration) = {


      def detect() = {
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

    def notEmpty() = view.neighbors.isEmpty != true


    def view_maintenance(interval: Duration) = {

      def exch = if (notEmpty) CLI.exchange_views(view) map {x => state.put(x) }

      timer.schedule(interval)(exch)
    }
  }



}


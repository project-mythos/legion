package Enki_DT.Proto

import Gossip.Types.{

  TPSet => TwoPSet, LWWEntry => LWWE,
  LWWSet => LWWSetP, Entry => OSE,
  ORSet => ORSetP, Dot => PDot
}

import Gossip.Messages.PV
import com.google.protobuf.ByteString
import Enki.PipeOps._
import Enki_DT._

trait DTCodec[T] {
  def encode(t: T): Array[Byte]
  def decode(b: Array[Byte]): T

  def toByteString(t: T) = encode(t) |> ByteString.copyFrom

  def fromByteString(bs: ByteString) =  bs.toByteArray |> decode 
  
}


class TPSetCodec[T](C: DTCodec[T]) extends DTCodec[TPSet[T]] {


  def encode(tp: TPSet[T]) = {
    val entriesB = tp.entries.map {e => C.toByteString(e)} 
    val tombstonesB = tp.tombstones map {t => C.toByteString(t)}
    TwoPSet(entriesB.toSeq, tombstonesB.toSeq).toByteArray
  }


  def decode(b: Array[Byte]) = {
    val pset = TwoPSet.parseFrom(b)

    val e = pset.writes.map {x => C.fromByteString(x)}
    val t = pset.deletes.map {x => C.fromByteString(x)}
    TPSet(e.toSet, t.toSet)
  }



}



trait PBBridge[T, U <: scalapb.GeneratedMessage with scalapb.Message[U] ] extends DTCodec[T] {
  val companion: scalapb.GeneratedMessageCompanion[U]

  def toPB(t: T): U
  def fromPB(u: U): T

  def encode(t: T) = toPB(t).toByteArray
  def decode(b: Array[Byte]) = companion.parseFrom(b) |> fromPB
}

class PBCodec[U  <: scalapb.GeneratedMessage with scalapb.Message[U]](C: scalapb.GeneratedMessageCompanion[U]) extends DTCodec[U] {

  def encode(t: U) = t.toByteArray
  def decode(b: Array[Byte]) = C.parseFrom(b)
}


class LWWEntryCodec[T](C: DTCodec[T]) extends PBBridge[LWWEntry[T], LWWE] {

  val companion = LWWE 

  def fromPB(u: LWWE) = {
    LWWEntry(C.fromByteString(u.element), u.ts)  
  }

  def toPB(t: LWWEntry[T]) = {
    val pl = C.toByteString(t.element)
    LWWE(pl, t.ts)
  }

}

class LWWSetCodec[T](C: DTCodec[T]) extends PBBridge[LWWSet[T], LWWSetP] {
  val EC = new LWWEntryCodec(C)
  val companion = LWWSetP

  def fromPB(ls: LWWSetP) = {
    val entries = ls.writes.map {e => EC.fromPB(e)} |> {res => res.toSet}
    val tombstones = ls.deletes.map {e => EC.fromPB(e)} |> {res => res.toSet}
    LWWSet(entries, tombstones)
  }


  def toPB(ls: LWWSet[T]) = {
    val writes = ls.entries.map {e => EC.toPB(e) } |> {res => res.toSeq }
    val deletes = ls.tombstones.map {e => EC.toPB(e)} |>  {res => res.toSeq}
    LWWSetP(writes, deletes)
  }
}





class EntryBridge[T](C: DTCodec[T]) extends PBBridge[Entry[T], OSE] {
  val companion = OSE
 

  def toPB(entry: Entry[T]) = {
    val element1 = C.encode(entry.element) |> ByteString.copyFrom
    val dot1 = PDot(entry.dot.id, entry.dot.counter)
    OSE(element1, dot1)
  }


  def fromPB(entry: OSE) = {
    val element1 = entry.element.toByteArray() |> C.decode
    val dot = Dot(entry.dot.id, entry.dot.counter)
    Entry(element1, dot)
  }
}



object DotBridge extends PBBridge[Dot, PDot] {
  val companion = PDot

  def toPB(dot: Dot) = {
    PDot(dot.id, dot.counter)
  }

  def fromPB(dot: PDot) = {
    Dot(dot.id, dot.counter)
  }
}



class ORSetBridge[T](codec: DTCodec[T]) extends PBBridge[ORSet[T], ORSetP] {

  val E = new EntryBridge(codec)
  val companion = ORSetP

  def toPB(orset: ORSet[T]) = {
    val entries = orset.entries.map {x => E.toPB(x) }
    val dots =  orset.dots.map(x => DotBridge.toPB(x) )
    val local = DotBridge.toPB(orset.local)

    ORSetP(local, entries.toSeq, dots.toSeq)
  }


  def fromPB(orset: ORSetP) = {
    val entries = orset.entries.map {x => E.fromPB(x) }.toSet
    val dots =  orset.dots.map(x => DotBridge.fromPB(x) ).toSet
    val local = DotBridge.fromPB(orset.local)
    ORSet(local, entries, dots)
  }

}

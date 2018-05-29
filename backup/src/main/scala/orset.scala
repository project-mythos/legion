package orset

import com.google.protobuf.ByteString
import protocol.types.types

trait Codec[T] {
  def encode(t: T): Array[Byte]
  def decode(t: Array[Byte]): T
}

case class Dot(id: String, counter: Long)

object Dot {

  def increment(dot: Dot) = {
    dot.copy(counter=dot.counter + 1 )
  }


  def eq(l: Dot, r: Dot) = {
    l.id == r.id & l.counter == r.counter 
  }


  def empty(id: String) = {
    Dot(id, 0L) 
  }


  def sorting(l: Dot, r: Dot): Boolean = {
    if (l.id != r.id) return l.id < r.id
    l.counter < r.counter 
  }

  def toProto(dot: Dot) = {
    types.Dot(dot.id, dot.counter)
  }

  def fromProto(dot: types.Dot) = {
    Dot(dot.id, dot.counter)
  }
}





case class Entry[T](element: T, dot: Dot)

object Entry {

  import legion.operators.PipeOps._

  def toProto[T](entry: Entry[T], codec: Codec[T]) = {
    val element1 = codec.encode(entry.element) |> ByteString.copyFrom
    val dot1 = types.Dot(entry.dot.id, entry.dot.counter)
    types.Entry(element1, dot1)
  }


  def fromProto[T](entry: types.Entry, codec: Codec[T]) = {
    val element1 = entry.element.toByteArray() |> codec.decode
    val dot = Dot(entry.dot.id, entry.dot.counter)
    Entry(element1, dot)
  }


}


case class ORSet[T](local: Dot, entries: Set[Entry[T]], dots: Set[Dot])



object ORSet {

  def empty[T](id: String) = {
    ORSet(
      Dot.empty(id),
      Set[Entry[T]](),
      Set[Dot]()
    )

  }

  def add[T](orset: ORSet[T], e: T) = {
    val local = Dot.increment(orset.local) 
    val entries = orset.entries + Entry(e, local)
    val dots = orset.dots + local

    orset.copy(local=local, entries=entries, dots=dots)
  }


  def delete[T](orset: ORSet[T], e: T) = {
    val local = Dot.increment(orset.local)
    val entries = orset.entries.filter (x => x.element != e)
    orset.copy(local=local, entries=entries) 
  }


  def query[T](orset: ORSet[T]): Set[T] = {
    orset.entries.map(x => x.element) 
  }


  def toAdd[T](l: ORSet[T], r: ORSet[T]) = {

    l.entries.filter {x =>
      val has_e = r.entries.contains(x) != true
      val has_d = r.dots.contains(x.dot) != true
      has_e && has_d 
    }

  }


  def toRemove[T](l: ORSet[T], r: ORSet[T]) = {

    l.entries.filter { x =>
      val is_dot = r.dots.contains(x.dot)
      val is_e = r.entries.contains(x) != true
      is_dot && is_e 
    }

  }

  def merge[T](l: ORSet[T], r: ORSet[T]) = {
    val e1 = toAdd(r, l) 
    val e2 = l.entries -- toRemove(l, r)

    val entries = e1 ++ e2
    val dots = l.dots ++  r.dots

    l.copy(entries=entries, dots=dots)
  }


}



class ORSetBridge[T](codec: Codec[T]) extends Codec[ORSet[T]] {

  def toProto(orset: ORSet[T]) = {
    val entries = orset.entries.map {x => Entry.toProto(x, codec) }
    val dots =  orset.dots.map(x => Dot.toProto(x) )
    val local = Dot.toProto(orset.local)
    types.ORSet(local, entries.toSeq, dots.toSeq)
  }


  def fromProto(orset: types.ORSet) = {
    val entries = orset.entries.map {x => Entry.fromProto(x, codec) }.toSet
    val dots =  orset.dots.map(x => Dot.fromProto(x) ).toSet
    val local = Dot.fromProto(orset.local)
    ORSet(local, entries, dots)
  }


  def encode(t: ORSet[T]) = {
    toProto(t).toByteArray
  }


  def decode(bytes: Array[Byte]) = {
    val p = types.ORSet.parseFrom(bytes)
    fromProto(p)
  }


}

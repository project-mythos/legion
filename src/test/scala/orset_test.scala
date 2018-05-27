import org.scalatest._
import legion.operators.PipeOps._
import orset._


class ORSetSuite extends FunSuite {

  val osA = ORSet.empty[String]("A")
  val osB = ORSet.empty[String]("B")


  val osA2 =
    ORSet.add(osA, "nigga") |> {x => ORSet.add(x, "Faggot") }

  val osB1 = ORSet.add(osB,"Bitch")


  def entries[T](os: ORSet[T]) = os.entries

  def sortedEntries[T](os: ORSet[T]) = {
    os.entries.toList.sortWith{ (l, r) =>  Dot.sorting(l.dot, r.dot) } 
  }

  test("Merges seperate writes") {
    val merged = ORSet.merge(osA2, osB1)
    assert(merged.entries.exists(x => x.element == "Bitch") )
    val (aEntries, mEntries) = (osA2.entries, merged.entries)
    assert(aEntries subsetOf mEntries)
  }



  test("Is it Commutative") {
    val mergedA = ORSet.merge(osA2, osB1)
    val mergedB = ORSet.merge(osB1, osA2)

    assert(sortedEntries(mergedA) == sortedEntries(mergedB) )
  }


  test("Observes deletes from other replicas") {
    val osA3 = ORSet.add(osA2, "Bish")

    val mergedA = ORSet.merge(osA3, osB1)
    val mergedB = ORSet.merge(osB1, osA3)

    val mergedB1 = ORSet.delete(mergedB, "Bish")

    val mergedA1 = ORSet.merge(mergedA, mergedB1)
    val p = ORSet.query(mergedA1).contains("Bish") != true
    assert(p)
  }


}

package Legion.GroupMembership

import scala.collection.concurrent.TrieMap
import Legion.PeerService
import Legion.Gossip._

object MemberState {
  type Path = List[String]

  trait S {
    val state: TrieMap[Path, PeerService]
    def create_group(path: Path): PeerService
  }


  class State(ST: S) {
    val state = S.state

    def join(grp: Path, node: )
  }


}

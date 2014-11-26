package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.util.Timeout

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  // var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def timeout = context.system.scheduler.scheduleOnce(100.milliseconds, self, Timeout)

  def sendSnapshot(key: String, valueOption: Option[String], seq: Long) = replica ! Snapshot(key, valueOption, seq)

  def receive: Receive = {
    case r: Replicate => {
      val seq = nextSeq
      acks += (seq -> (sender, r))
      sendSnapshot(r.key, r.valueOption, seq)
      timeout
    }
    case s: SnapshotAck => {
      val (primary, replicate) = acks(s.seq)
      acks -= s.seq
      primary ! Replicated(s.key, replicate.id)
    }
    case Timeout => {
      if (!acks.isEmpty) {
        acks.foreach { case (seq, (primary, replicate)) => sendSnapshot(replicate.key, replicate.valueOption, seq) }
        timeout
      }
    }
  }

}

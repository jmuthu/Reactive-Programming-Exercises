package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Cancellable
import scala.concurrent.duration._

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
  var acks = Map.empty[Long, (ActorRef, Replicate, Cancellable)]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  
  override def postStop = {
   acks.foreach{case (id,(a,r,schedule)) => schedule.cancel}
  }

  def receive: Receive = {
    case r: Replicate => {
      val seq = nextSeq
      val schedule = context.system.scheduler.schedule(0.milliseconds, 100.milliseconds, replica, Snapshot(r.key, r.valueOption, seq))
      acks += (seq -> (sender, r, schedule))
    }
    case s: SnapshotAck => if (acks contains s.seq) {
      val (primary, replicate, schedule) = acks(s.seq)
      schedule.cancel
      acks -= s.seq
      primary ! Replicated(s.key, replicate.id)
    }
  }

}

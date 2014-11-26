package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class FailedTimeout(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  // Variables to hold state of the Actor
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // the list of actors waiting for ack
  var pendingAck = Map.empty[Long, (ActorRef, String, Option[String], Set[ActorRef])]
  // variable to create ids for sending replicate messages
  var _seqCounter = -1L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter -= 1
    ret
  }
  // variable to store the expected sequence Number from the replicate for the 
  // secondary actor
  var expectedNumber: Long = 0;

  // Do initial start up work. Send Ack to Arbiter, Create persistence actor 
  // set restart strategy
  arbiter ! Join

  val persistence = context.actorOf(persistenceProps)
  context.watch(persistence)

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10) {
    case _: Exception => SupervisorStrategy.Restart
  }

  // Changing states of Pending variable
  def insertPendingAck(key: String, valueOption: Option[String], id: Long, children: Set[ActorRef]) {
    if (pendingAck contains id) {
      val (source, key, value, list) = pendingAck(id)
      pendingAck += (id -> (source, key, value, list ++ children))
    } else
      pendingAck += (id -> (sender, key, valueOption, children))
  }

  def removePendingAck(id: Long, child: ActorRef, ack: Option[Any] = None) = {
    if (pendingAck contains id) {
      val (source, key, value, list) = pendingAck(id)
      val newList = list - child
      if (newList.isEmpty)
        removePendingId(id, source, ack)
      else {
        pendingAck += (id -> (source, key, value, newList))
      }
    }
  }

  def removePendingId(id: Long, source: ActorRef, ack: Option[Any]) = {
    pendingAck -= id
    ack match {
      case Some(a) => source ! a
      case None =>
    }
  }

  def removePendingAllAck(id: Long, ack: Option[Any]) = {
    if (pendingAck.contains(id)) {
      val (source, key, value, list) = pendingAck(id)
      removePendingId(id, source, ack)
    }
  }

  def removeAllPendingAck(receiver: ActorRef)(response: Long => Any) = {
    pendingAck.foreach { case (id, (s, k, v, l)) => removePendingAck(id, receiver, Option(response(id))) }
    context.unwatch(receiver)
  }

  // Helpers for receive messages

  // Get Helper
  def get(key: String, id: Long) = sender ! GetResult(key, kv.get(key), id)

  // Persist Helpers
  def persisted(id: Long, ack: Any) = removePendingAck(id, persistence, Option(ack))

  def setTimeoutForPersistAck = context.system.scheduler.scheduleOnce(100.millisecond, self, Timeout)

  def persist(key: String, valueOption: Option[String], id: Long) = {
    insertPendingAck(key, valueOption, id, Set(persistence))
    valueOption match {
      case Some(v) => kv += (key -> v)
      case None => kv = kv - key
    }
    persistence ! Persist(key, valueOption, id)
    setTimeoutForPersistAck
  }

  def persistTimeout = {
    if (!pendingAck.isEmpty) {
      pendingAck.foreach {
        case (id, (source, key, valueOption, repList)) => if (repList contains persistence) persistence ! Persist(key, valueOption, id)
      }
      setTimeoutForPersistAck
    }
  }

  // Replicate Helpers
  def replicate(key: String, valueOption: Option[String], id: Long, reps: Set[ActorRef]) = {
    insertPendingAck(key, valueOption, id, reps)
    reps.foreach(_ ! Replicate(key, valueOption, id))
  }

  def handleReplicas(replicas: Set[ActorRef]) = {
    val newReplicas = (replicas - self) -- secondaries.keys
    val terminatedReplicas = secondaries.keys.toSet -- replicas

    val newReplicators = for (r <- newReplicas) yield {
      val replicator = context.actorOf(Replicator.props(r))
      replicators += replicator
      secondaries += (r -> replicator)
      context.watch(replicator)
      replicator
    }
    for ((k, v) <- kv) yield {
      val updateId = nextSeq
      replicate(k, Option(v), updateId, newReplicators)
    }

    for (replica <- terminatedReplicas) yield {
      val replicator = secondaries(replica)
      secondaries -= replica
      replicators -= replicator
      removeAllPendingAck(replicator)(OperationAck(_))
      context.stop(replicator)
    }
  }

  // Terminate Helpers

  // Updates from clients
  def handleUpdates(key: String, valueOption: Option[String], id: Long) = {
    persist(key, valueOption, id)
    replicate(key, valueOption, id, replicators)
    context.system.scheduler.scheduleOnce(1.second, self, FailedTimeout(id))
  }

  // Receive Functions 

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = {
    // Request
    case g: Get => get(g.key, g.id)
    case i: Insert => handleUpdates(i.key, Option(i.value), i.id)
    case r: Remove => handleUpdates(r.key, None, r.id)
    case rep: Replicas => handleReplicas(rep.replicas)

    // Responses
    case p: Persisted => persisted(p.id, OperationAck(p.id))
    case replicated: Replicated => removePendingAck(replicated.id, sender, Option(OperationAck(replicated.id)))

    // Exceptions
    case Timeout => persistTimeout
    case FailedTimeout(id) => removePendingAllAck(id, Option(OperationFailed(id)))
    case Terminated(a) => removeAllPendingAck(a)(OperationFailed(_))
  }

  val replica: Receive = {
    // Request
    case g: Get => get(g.key, g.id)
    case s: Snapshot => {
      if (expectedNumber > s.seq)
        sender ! SnapshotAck(s.key, s.seq)
      else if (expectedNumber == s.seq) {
        expectedNumber += 1
        persist(s.key, s.valueOption, s.seq)
      }
    }

    // Responses
    case p: Persisted => persisted(p.id, SnapshotAck(p.key, p.id))

    // Exceptions
    case Timeout => persistTimeout
    case Terminated(a) => {
      removeAllPendingAck(a)(_ => None)
      context.stop(self)
    }
  }

}

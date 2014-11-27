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
import akka.actor.Cancellable

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

  class Ack(val source: ActorRef, val receivers: Set[ActorRef], val persistOpSchedule: Option[Cancellable], val failedOpSchedule: Option[Cancellable])

  // Variables to hold state of the Actor
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // the list of actors waiting for ack
  var pendingAck = Map.empty[Long, Ack]
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
  def insertPendingAck(id: Long,
    children: Set[ActorRef],
    persistScheduleOption: Option[Cancellable] = None,
    failedScheduleOption: Option[Cancellable] = None) {
    if (pendingAck contains id) {
      val ack = pendingAck(id)
      pendingAck += (id -> new Ack(ack.source, ack.receivers ++ children, ack.persistOpSchedule, ack.failedOpSchedule))
    } else
      pendingAck += (id -> new Ack(sender, children, persistScheduleOption, failedScheduleOption))
  }

  def removePendingReceiver(id: Long, receiver: ActorRef)(implicit response: (ActorRef, Long) => Unit) = if (pendingAck contains id) {
    val ack = pendingAck(id)
    val newList = ack.receivers - receiver
    if (newList.isEmpty)
      removePendingId(id, ack)
    else if (persistence == receiver) {
      ack.persistOpSchedule.get.cancel
      pendingAck += (id -> new Ack(ack.source, newList, None, ack.failedOpSchedule))
    } else if (newList.size == 1 && newList.contains(persistence) && ack.failedOpSchedule.isDefined) {
      ack.failedOpSchedule.get.cancel
      pendingAck += (id -> new Ack(ack.source, newList, ack.persistOpSchedule, None))
    } else
      pendingAck += (id -> new Ack(ack.source, newList, ack.persistOpSchedule, ack.failedOpSchedule))
  }

  def removePendingId(id: Long, ack: Ack)(implicit response: (ActorRef, Long) => Unit) = {
    pendingAck -= id
    if (ack.failedOpSchedule.isDefined) ack.failedOpSchedule.get.cancel
    if (ack.persistOpSchedule.isDefined) ack.persistOpSchedule.get.cancel
    response(ack.source, id)
  }

  // Helpers for receive messages

  // Get Helper
  def get(key: String, id: Long) = sender ! GetResult(key, kv.get(key), id)

  // Persist Helpers
  def persist(key: String,
    valueOption: Option[String],
    id: Long,
    failedOpSchedule: Option[Cancellable] = None) = {

    valueOption match {
      case Some(v) => kv += (key -> v)
      case None => kv = kv - key
    }
    val persistOpSchedule = context.system.scheduler.schedule(0.milliseconds, 100.milliseconds, persistence, Persist(key, valueOption, id))
    insertPendingAck(id, Set(persistence), Option(persistOpSchedule), failedOpSchedule)
  }

  // Replicate Helpers
  def replicate(key: String, valueOption: Option[String], id: Long, reps: Set[ActorRef]) = {
    insertPendingAck(id, reps)
    reps.foreach(_ ! Replicate(key, valueOption, id))
  }

  // Updates from clients
  def handleUpdates(key: String, valueOption: Option[String], id: Long) = {
    val failedOpSchedule = context.system.scheduler.scheduleOnce(1.second, self, FailedTimeout(id))
    persist(key, valueOption, id, Option(failedOpSchedule))
    replicate(key, valueOption, id, replicators)
  }

  // Handle Arbiter replica
  def handleReplicas(replicas: Set[ActorRef]) = {
    val newReplicas = (replicas - self) -- secondaries.keys
    val terminatedReplicas = secondaries.keys.toSet -- replicas

    val newReplicators = for (r <- newReplicas) yield {
      val replicator = context.actorOf(Replicator.props(r))
      secondaries += (r -> replicator)
      context.watch(replicator)
      replicator
    }
    replicators ++= newReplicators

    for ((k, v) <- kv) yield {
      val updateId = nextSeq
      replicate(k, Option(v), updateId, newReplicators)
    }

    for (replica <- terminatedReplicas) yield {
      val replicator = secondaries(replica)
      secondaries -= replica
      replicators -= replicator
      pendingAck.foreach { case (id, ack) => removePendingReceiver(id, replicator)((source, id) => source ! OperationAck(id)) }
      context.unwatch(replicator)
      context.stop(replicator)
    }
  }

  // Receive Functions 

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def common(stopOnPersistTermination: Boolean, ack: (String, Long) => Any): Receive = {
    // Request
    case g: Get => get(g.key, g.id)
    // Responses
    case p: Persisted => removePendingReceiver(p.id, persistence)((source, id) => source ! ack(p.key, id))
    // Exceptions
    case Terminated(persister) => {
      context.unwatch(persister)
      if (stopOnPersistTermination) {
        pendingAck.foreach { case (id, ack) => removePendingReceiver(id, persister)((_, _) => ()) }
        context.stop(self)
      } else {
        pendingAck.foreach { case (id, ack) => removePendingReceiver(id, persister)((source, id) => source ! OperationFailed(id)) }
      }

    }

  }

  val leader: Receive = common(false, (key, id) => OperationAck(id)) orElse {
    // Request
    case i: Insert => handleUpdates(i.key, Option(i.value), i.id)
    case r: Remove => handleUpdates(r.key, None, r.id)
    case rep: Replicas => handleReplicas(rep.replicas)

    // Responses
    case replicated: Replicated => removePendingReceiver(replicated.id, sender)((source, id) => source ! OperationAck(id))

    // Exceptions
    case FailedTimeout(id) => if (pendingAck.contains(id)) {
      removePendingId(id, pendingAck(id))((source, id) => source ! OperationFailed(id))
    }
  }

  val replica: Receive = common(true, SnapshotAck) orElse {
    // Request
    case s: Snapshot => if (expectedNumber > s.seq) {
      sender ! SnapshotAck(s.key, s.seq)
    } else if (expectedNumber == s.seq) {
      expectedNumber += 1
      persist(s.key, s.valueOption, s.seq)
    }
  }

}

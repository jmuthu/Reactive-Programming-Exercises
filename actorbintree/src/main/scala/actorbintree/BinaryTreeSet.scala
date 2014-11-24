/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue
import akka.event.LoggingReceive

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /**
   * Request with identifier `id` to insert an element `elem` into the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
   * Request with identifier `id` to check whether an element `elem` is present
   * in the tree. The actor at reference `requester` should be notified when
   * this operation is completed.
   */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
   * Request with identifier `id` to remove the element `elem` from the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /**
   * Holds the answer to the Contains request with identifier `id`.
   * `result` is true if and only if the element is present in the tree.
   */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case o: Operation => root ! o
    case GC => {
      var newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
  }

  // optional
  /**
   * Handles messages while garbage collection is performed.
   * `newRoot` is the root of the new binary tree where we want to copy
   * all non-removed elements into.
   */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC =>
    case o: Operation => pendingQueue = pendingQueue.enqueue(o)
    case CopyFinished => {
      context.stop(root)
      root = newRoot
      pendingQueue.foreach(root ! _)
      pendingQueue = Queue.empty[Operation]
      context.become(normal)
    }
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  def getPos(e: Int): Position = if (e < elem) Left else Right

  def isNodeEmpty(pos:Position): Boolean = subtrees.isEmpty || !subtrees.contains(pos)

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {

    case c: Contains => {
      val pos = getPos(c.elem)

      if (c.elem == elem)
        c.requester ! ContainsResult(c.id, !removed)
      else if (isNodeEmpty(pos))
        c.requester ! ContainsResult(c.id, false)
      else
        subtrees(pos) ! c

    }

    case i: Insert => {
      val pos = getPos(i.elem)

      if (i.elem == elem) {
        if (removed) removed = false
        i.requester ! OperationFinished(i.id)
      } else if (isNodeEmpty(pos)) {
        subtrees += (pos -> context.actorOf(props(i.elem, false)))
        i.requester ! OperationFinished(i.id)
      } else
        subtrees(pos) ! i

    }

    case r: Remove => {
      val pos = getPos(r.elem)

      if (r.elem == elem) {
        removed = true
        r.requester ! OperationFinished(r.id)
      } else if (isNodeEmpty(pos))
        r.requester ! OperationFinished(r.id)
      else
        subtrees(pos) ! r
    }

    case copyTo: CopyTo => {
      if (!removed)
        copyTo.treeNode ! Insert(self, 1, elem)
      val expected = subtrees.values.toSet
      expected.foreach { _ ! copyTo }
      confirmDone(expected, removed)
    }
  }

  def confirmDone(expected: Set[ActorRef], insertConfirmed: Boolean) = {
    if (insertConfirmed && expected.isEmpty)
      context.parent ! CopyFinished
    else
      context.become(copying(expected, insertConfirmed))
  }

  // optional
  /**
   * `expected` is the set of ActorRefs whose replies we are waiting for,
   * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
   */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case o: OperationFinished => confirmDone(expected, true)
    case CopyFinished => confirmDone(expected - sender, insertConfirmed)
  }

}
 
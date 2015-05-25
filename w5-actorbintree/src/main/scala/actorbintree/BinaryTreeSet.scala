/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
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
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = { 
    case o: Operation => pendingQueue.enqueue(o)
    case CopyFinished => {
      while (!pendingQueue.isEmpty) {
        val (o, q) = pendingQueue.dequeue
        newRoot ! o
        pendingQueue = q
      }
      root ! PoisonPill
      root = newRoot
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

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = { 
    
    case Contains(requester: ActorRef, elId: Int, el: Int) => {
      if (el > elem) {
        if (subtrees.contains(Right)) subtrees(Right) ! Contains(requester, elId, el)
        else requester ! ContainsResult(elId, false)
      } else if (el < elem) {
        if (subtrees.contains(Left)) subtrees(Left) ! Contains(requester, elId, el)
        else requester ! ContainsResult(elId, false)
      } else requester ! ContainsResult(elId, !removed)
    }
      
    case Insert(requester: ActorRef, elId: Int, el: Int) =>
      if (el > elem) {
        subtrees.get(Right) match {
          case Some(rightNode) => rightNode ! Insert(requester, elId, el)
          case None => {
            subtrees += Right -> context.actorOf(BinaryTreeNode.props(el, false))
            requester ! OperationFinished(elId)
          }
        }
      } else if (el < elem) {
        subtrees.get(Left) match {
          case Some(leftNode) => leftNode ! Insert(requester, elId, el)
          case None => {
            subtrees += Left -> context.actorOf(BinaryTreeNode.props(el, false))
            requester ! OperationFinished(elId)
          }
        }
      } else {
        removed = false;
        requester ! OperationFinished(elId)
      }
      
    case Remove(requester: ActorRef, elId: Int, el: Int) =>
      if (el > elem) {
        subtrees.get(Right) match {
          case Some(rightNode) => rightNode ! Remove(requester, elId, el)
          case None => requester ! OperationFinished(elId)
        }
      } else if (el < elem) {
        subtrees.get(Left) match {
          case Some(leftNode) => leftNode ! Remove(requester, elId, el)
          case None => requester ! OperationFinished(elId)
        }
      } else {
        removed = true;
        requester ! OperationFinished(elId)
      }
        
    case CopyTo(treeNode: ActorRef) => {
      if (removed && subtrees.isEmpty) {
        context.parent ! CopyFinished
      } else {
        if (!removed) treeNode ! Insert(self, elem, elem)
        val children = subtrees.values.toSet
        children.foreach { a => a ! CopyTo(treeNode) }
        context.become(copying(children, false))
      }
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) => {
      if (expected.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
      } else {
        context.become(copying(expected, true))
      }
    }
    case CopyFinished => {
      val rest = expected - sender
      if (rest.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
      } else {
        context.become(copying(rest, insertConfirmed))
      }
    }
  }
}

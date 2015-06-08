package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.event.LoggingReceive
import language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case object Resend
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  import context.dispatcher
  val tick =
    context.system.scheduler.schedule(100.millis, 100.millis, self, Resend)

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = LoggingReceive {
    case Replicate(key, value, id) =>
      val nSeq = nextSeq
      replica ! Snapshot(key, value, nSeq)
      acks += (nSeq -> (sender, Replicate(key, value, id)))
    case SnapshotAck(key, seq) =>
      acks.get(seq) match {
        case Some((sender, Replicate(_, _, id))) =>
          sender ! Replicated(key, id)
          acks -= seq
        case None => ;
      }
    case Resend => 
      acks foreach {
        case (seq, (sender, Replicate(key, value, id))) =>
          replica ! Snapshot(key, value, seq)
      }

  }
}

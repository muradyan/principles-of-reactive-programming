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
import scala.collection.mutable.{Map => MMap}
import akka.event.LoggingReceive
import language.postfixOps

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

  case class CheckIfPersisted(seq: Long)
  case class CheckIfReplicatedAndPersisted(persistId: Long, idsAndReplicators: Map[Long, ActorRef])
  
  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher
  
  private var seq = 0L
  private def nextSeq: Long = {
    val next = seq
    seq += 1
    next
  }

  var nextSnapshot = 0
  val persistenceActor = context.actorOf(persistenceProps, s"persistence")

  arbiter ! Join

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var acks = Map.empty[Long, (ActorRef, Snapshot)]
  
  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  var persistAcks = Map.empty[Long, (ActorRef, Operation)]
  var persisted = List[Long]()
  var outstandingReplicationAcks = Map[Long, ActorRef]()

  def update(op: Operation) = {

    val valueOption = op match {
      case Insert(key, value, id) =>
        persistAcks += (id -> (sender, Insert(key, value, id)))
        Some(value)
      case Remove(key, id) =>
        persistAcks += (id -> (sender, Remove(key, id)))
        None
      case Get(_, _) => None
    }

    persistenceActor ! Persist(op.key, valueOption, op.id)
    context.system.scheduler.scheduleOnce(100 millis, self, CheckIfPersisted(op.id))

    val replicationIdsAndReplicators = replicators map { r =>
      val newId = nextSeq
      r ! Replicate(op.key, valueOption, newId)
      (newId, r)
    } toMap;

    context.system.scheduler.scheduleOnce(990 millis, self, CheckIfReplicatedAndPersisted(op.id, replicationIdsAndReplicators))
    outstandingReplicationAcks ++= replicationIdsAndReplicators
  }

  /* TODO Behavior for the leader role. */
  val leader: Receive = LoggingReceive {
    case Insert(key, value, id) =>
      kv += (key -> value)
      update(Insert(key, value, id))
    case Remove(key, id) =>
      kv -= key
      update(Remove(key, id))
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Replicas(all) =>
      val joined = all &~ (secondaries.keys.toSet + self)
      val left = (secondaries.keys.toSet + self) &~ all

      val newReplicasWithReplicators = joined map {
        n => (n, context.actorOf(Replicator.props(n)))
      }
      val newReplicators = newReplicasWithReplicators map (x => x._2)
      secondaries ++= newReplicasWithReplicators
      replicators ++= newReplicators
      newReplicators foreach {
        r => kv foreach { case (key, value) => r ! Replicate(key, Some(value), nextSeq) }
      }
      val leftReplicators = left map (x => secondaries(x))
      outstandingReplicationAcks = outstandingReplicationAcks filter {
        case (id, replicator) => !leftReplicators.contains(replicator)
      }    
      secondaries --= left
      replicators --= leftReplicators
      leftReplicators foreach (r => context.stop(r))
    case Replicated(key, id) =>
      outstandingReplicationAcks -= id
    case Persisted(key, id) =>
      persisted = id :: persisted
    case CheckIfPersisted(id) =>
      if (!persisted.contains(id)) {
        persistAcks.get(id) map {case (sender, op) =>
            val valueOption = op match {
              case Insert(key, value, id) =>
                Some(value)
              case Remove(key, id) =>
                None
              case Get(_, _) => None
            }
            persistenceActor ! Persist(op.key, valueOption, id)
            context.system.scheduler.scheduleOnce(100 millis, self, CheckIfPersisted(id))
        }
      }
    case CheckIfReplicatedAndPersisted(persistId, idsAndReplicators) =>
      val outstandingReplications = idsAndReplicators.keys.toSet & outstandingReplicationAcks.keys.toSet
      persistAcks.get(persistId) foreach {
        case (sender, op) =>
          if (persisted.contains(persistId) && outstandingReplications.isEmpty) {
            sender ! OperationAck(persistId)
          } else {
            sender ! OperationFailed(persistId)            
          }
          persistAcks -= persistId
      }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = LoggingReceive {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Snapshot(key, value, seq) =>
      if (seq < nextSnapshot) {
        sender ! SnapshotAck(key, seq)
      } else if (seq == nextSnapshot) {
        value match {
          case Some(v) => kv += (key -> v)
          case None => kv -= key
        }
        persistenceActor ! Persist(key, value, seq)
        acks += (seq -> (sender, Snapshot(key, value, seq)))
        context.system.scheduler.scheduleOnce(100 millis, self, CheckIfPersisted(seq))
      }
    case Persisted(key, id) =>
      acks.get(id) match {
        case None =>
        case Some((sender, Snapshot(key, value, seq))) =>
          sender ! SnapshotAck(key, seq)
          nextSnapshot += 1
          acks -= id
      }
    case CheckIfPersisted(seq) =>
      acks.get(seq) match {
        case None => ;
        case Some((sender, Snapshot(key, value, seq))) =>
          persistenceActor ! Persist(key, value, seq)
          context.system.scheduler.scheduleOnce(100 millis, self, CheckIfPersisted(seq))
      }
  }

  override val supervisorStrategy = OneForOneStrategy() {
    case _: PersistenceException => Restart
  }
}


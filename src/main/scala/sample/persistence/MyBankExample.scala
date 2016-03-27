package sample.persistence

import akka.actor.{ActorPath, ActorSystem, Props}
import akka.persistence._

/**
  * Created by greghuang on 3/26/16.
  **/
sealed trait Transaction

case class Withdraw(money: Int) extends Transaction

case class Deposit(money: Int) extends Transaction

case class Transfer(money: Int, destination: ActorPath) extends Transaction

case object Query extends Transaction

case object Snapshot extends Transaction

case class DeliveryCmd(deliveryId: Long, money: Int)

case class ConfirmedCmd(deliveryId: Long)

case class AmountEvt(amount: Int)

sealed trait TransactionEvt

case class MoneySentEvt(money: Int, destination: ActorPath) extends TransactionEvt

case class MoneyConfrimedEvt(deliveryId: Long) extends TransactionEvt


case class Balance(account: String) {
  val accountName = account
  var balance: Int = 0

  def update(evt: AmountEvt): Unit = balance = balance + evt.amount

  def deposits: Int = balance

  override def toString: String = s"$accountName has NTD $balance now"
}

object BankAccountActor {
  def props(name: String): Props = Props(new BankAccountActor(name))
}

class BankAccountActor(name: String) extends PersistentActor with AtLeastOnceDelivery {
  override def persistenceId: String = s"$name-account-1"

  val accountName = name
  var myBalance = Balance(accountName)
  var lastSnapshot: SnapshotMetadata = _

  //  override def recovery = Recovery(fromSnapshot = SnapshotSelectionCriteria(
  //    maxSequenceNr = 7L,
  //    maxTimestamp =  System.currentTimeMillis()
  //  ))

  override def receiveRecover: Receive = {
    case evt: AmountEvt => {
      println(s"$accountName balance: " + evt.amount)
      myBalance.update(evt)
    }
    case evtTranc: TransactionEvt => transfer(evtTranc)
    case SnapshotOffer(metadata, offeredSnapshot: Balance) => {
      println("Recovery by " + metadata)
      lastSnapshot = metadata
      myBalance = offeredSnapshot
    }
    case RecoveryCompleted => println(s"$accountName account recovery is done!")
  }

  override def saveSnapshot(snapshot: Any): Unit = {
    //delete old one
    if (lastSnapshot != null) {
      println("\tDelete old snapshot by " + lastSnapshot)
      deleteSnapshot(lastSnapshot.sequenceNr)
    }

    print("\tSave snapshot...")
    super.saveSnapshot(snapshot)
    println("Done")

    println("\tDelete messages to Last seqNr " + lastSequenceNr)
    deleteMessages(lastSequenceNr)
  }

  //  def Transaction: Receive = {
  //    case Transfer(money, destination) => {
  //      persist(AmountEvt(-money))(myBalance.update)
  //      persist(MoneySentEvt(money, destination))(transfer)
  //    }
  //    case ConfirmedCmd(deliveryId) => {
  //      persist(MoneyConfrimedEvt(deliveryId))(transfer)
  //    }
  //    case _ => stash()
  //  }

  def transfer(evt: TransactionEvt): Unit = evt match {
    case MoneySentEvt(money, destination) =>
      deliver(destination)(deliveryId => DeliveryCmd(deliveryId, money))
    case MoneyConfrimedEvt(deliveryId) =>
      confirmDelivery(deliveryId)
  }

  def checkDeposits(money: Int): Boolean = money <= myBalance.deposits && money > 0

  override def receiveCommand: Receive = {
    case Withdraw(money) if checkDeposits(money) => persist(AmountEvt(-money))(myBalance.update)
    case Deposit(money) if money > 0 => persist(AmountEvt(money))(myBalance.update)
    case Transfer(money, destination) if checkDeposits(money) => {
      persist(AmountEvt(-money))(myBalance.update)
      persist(MoneySentEvt(money, destination))(transfer)
    }
    case DeliveryCmd(deliveryId, money) if money > 0 => {
      persist(AmountEvt(money))(myBalance.update)
      sender() ! ConfirmedCmd(deliveryId)
    }
    case ConfirmedCmd(deliveryId) => {
      persist(MoneyConfrimedEvt(deliveryId))(transfer)
    }
    case Query => println(myBalance.toString)
    case Snapshot => saveSnapshot(myBalance)
    case SaveSnapshotSuccess(metadata) => println("Save snapshot successfully in " + metadata)
    case SaveSnapshotFailure(metadata, reason) => println(reason)
    case _ => sender ! akka.actor.Status.Failure(new RuntimeException("transaction failed"))
  }
}

object MyBankExample extends App {
  val actorSys = ActorSystem("bank")
  val myBankAccount = actorSys.actorOf(BankAccountActor.props("Greg"), "MyBankAccount-Test-01")
  val zabbyAccount = actorSys.actorOf(BankAccountActor.props("Zabby"), "MyBankAccount-Test-02")

  //  myBankActor ! Deposit(200)
  //  myBankActor ! Deposit(50)
  //  myBankActor ! Withdraw(120)
  //  myBankActor ! Withdraw(10000)
  //myBankAccount ! Snapshot
  myBankAccount ! Transfer(100, zabbyAccount.path)
  //  zabbyAccount! Transfer(50, myBankAccount.path)

  Thread.sleep(1000)
  myBankAccount ! Query
  zabbyAccount ! Query

  Thread.sleep(1000)
  actorSys.terminate()
}

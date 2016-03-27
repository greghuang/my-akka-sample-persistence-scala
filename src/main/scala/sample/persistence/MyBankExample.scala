package sample.persistence

import akka.actor.{ActorPath, ActorSystem, Props, Stash}
import akka.persistence._

/**
  * Created by greghuang on 3/26/16.
  **/
sealed trait AccountOp

case class Withdraw(money: Int) extends AccountOp

case class Deposit(money: Int) extends AccountOp

case object Transaction extends AccountOp

case object Query extends AccountOp

sealed trait TransactionOp

case class TransferCmd(money: Int, destination: ActorPath) extends TransactionOp

case class DeliveryCmd(deliveryId: Long, money: Int) extends TransactionOp

case class ConfirmedCmd(deliveryId: Long) extends TransactionOp

case class AmountEvt(amount: Int)

sealed abstract class TransactionEvt
case class MoneySentEvt(money: Int, destination: ActorPath) extends TransactionEvt
case class MoneyConfrimedEvt(deliveryId: Long) extends TransactionEvt

case object Snapshot

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

class BankAccountActor(name: String) extends PersistentActor with AtLeastOnceDelivery with Stash {

  import context._
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
    case RecoveryCompleted => println(s"Recovery is done, $accountName has ${myBalance.deposits}!")
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

  def transfer(evt: TransactionEvt): Unit = evt match {
    case MoneySentEvt(money, destination) =>
      deliver(destination)(deliveryId => DeliveryCmd(deliveryId, money))
    case MoneyConfrimedEvt(deliveryId) =>
      confirmDelivery(deliveryId)
  }

  def checkDeposits(money: Int): Boolean = money <= myBalance.deposits && money > 0

  def doTransaction: Receive = {
    case TransferCmd(money, destination) if checkDeposits(money) => {
      println(s"Transfer money...($accountName)")
      persist(AmountEvt(-money))(myBalance.update)
      persist(MoneySentEvt(money, destination))(transfer)
    }
    case ConfirmedCmd(deliveryId) => {
      println(s"Confirmed...($accountName)")
      persist(MoneyConfrimedEvt(deliveryId))(transfer)
      unstashAll()
      unbecome()
    }
    case DeliveryCmd(deliveryId, money) if money > 0 => {
      println(s"Receive money...($accountName)")
      persist(AmountEvt(money))(myBalance.update)
      sender() ! ConfirmedCmd(deliveryId)
      unstashAll()
      unbecome()
    }
    case _ => stash()
  }

  override def receiveCommand: Receive = {
    case Withdraw(money) if checkDeposits(money) => persist(AmountEvt(-money))(myBalance.update)
    case Deposit(money) if money > 0 => persist(AmountEvt(money))(myBalance.update)
    case Transaction => become(doTransaction)
    case Query => println(myBalance.toString)
    case Snapshot => saveSnapshot(myBalance)
    case SaveSnapshotSuccess(metadata) => println("Save snapshot successfully in " + metadata)
    case SaveSnapshotFailure(metadata, reason) => println(reason)
    case _ => //sender ! akka.actor.Status.Failure(new RuntimeException("transaction failed"))
  }
}

object MyBankExample extends App {
  val actorSys = ActorSystem("bank")
  val myBankAccount = actorSys.actorOf(BankAccountActor.props("Greg"), "MyBankAccount-Test-01")
  val zabbyAccount = actorSys.actorOf(BankAccountActor.props("Zabby"), "MyBankAccount-Test-02")

  //  myBankAccount ! Deposit(1000)
  //  myBankAccount ! Deposit(50)
  //  myBankAccount ! Withdraw(200)
  //  myBankActor ! Withdraw(10000)
  //  myBankAccount ! Snapshot
  myBankAccount ! Transaction
  zabbyAccount ! Transaction
  Thread.sleep(100)
  myBankAccount ! TransferCmd(50, zabbyAccount.path)

  Thread.sleep(1000)
  myBankAccount ! Query
  zabbyAccount ! Query
  myBankAccount ! Snapshot
  zabbyAccount ! Snapshot

  Thread.sleep(1000)
  actorSys.terminate()
}

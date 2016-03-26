package sample.persistence

import akka.actor.{ActorSystem, Props}
import akka.persistence.PersistentActor

/**
  * Created by greghuang on 3/26/16.
 **/
sealed abstract class Transaction
case class Withdraw(money: Int) extends Transaction
case class Deposit(money: Int) extends Transaction
case class Transfer(money: Int) extends Transaction
case object Query extends Transaction


case class AmountEvt(amount: Int)

case class Balance(account: String) {
  val accountName = account
  var balance: Int = 0
  def update (evt: AmountEvt): Unit = balance = balance + evt.amount
  override def toString: String = s"$accountName has NTD $balance now"
}

class BankAccountActor extends PersistentActor {
  override def persistenceId: String = "My-Account-1"

  val myBalance = Balance("Greg")

  override def receiveRecover: Receive = {
    case evt: AmountEvt => myBalance.update(evt)
  }

  override def receiveCommand: Receive = {
    case Withdraw(money) => persist(AmountEvt(-money))(myBalance.update)
    case Deposit(money) => persist(AmountEvt(money))(myBalance.update)
    case Transfer(money) => persist(AmountEvt(-money))(myBalance.update)
    case Query => println(myBalance.toString)
  }
}

object MyBankExample extends App {
  val actorSys = ActorSystem("bank")
  val myBankActor = actorSys.actorOf(Props[BankAccountActor], "MyBankAccount-Test-1")

  myBankActor ! Deposit(100)
  myBankActor ! Deposit(50)
  myBankActor ! Withdraw(90)
  myBankActor ! Query

  Thread.sleep(1000)
  actorSys.terminate()
}

package examples

import akka.actor.{Inbox, ActorSystem, Actor, Props}

/**
  * Created by stevekludt on 1/15/16.
  */

/** this is the akka code in it's own file...
case object Reading
case class Sensor(deviceID: String, date: String, time: String, temp: Double, humid: Double, flo: Double, co2: Double, psi: Double, chlPPM: Double)

class SparkMessage extends Actor {
  var count = 0
  var msg = ""
  def incrementAndPrint {
    count += 1
    println("sensor message received by akka " + msg + " message # " + count)
  }
  /**
    * this is where we define what we want to do whenever we receive a message
    * if we are getting multiple message types we can break them out here with the
    * case statement by the class that is passed in by adding additional case statements
    */
  def receive = {
    case Sensor(deviceID, date, time) =>
      msg = deviceID + ' ' + date + ' ' + time
      incrementAndPrint //this is where I can put logic to separate messages by customer and/or device
  }
}

object sparkAkka extends App {

  //Create the actor system
  val system = ActorSystem("sparkforweb")

  //create the actor
  val message = system.actorOf(Props[SparkMessage], "message")

  //create an "actor-in-a-box"
  // this is a shortcut because I'm only creating 1 actor right now, we need to have more in production
  val inbox = Inbox.create(system)


}

*/
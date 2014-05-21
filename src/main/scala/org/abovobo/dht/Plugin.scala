package org.abovobo.dht

import org.abovobo.dht.Plugin.PID
import akka.actor.Actor

abstract class Plugin(protected val pid: PID) extends Actor {
  
}

object Plugin {
  class PID(val number: Long) {
    if (number < 0 || number > 255) throw new IllegalArgumentException();
    
    override def toString = number.toString
  }  
  
  val SearchPluginId = new PID(1)
}
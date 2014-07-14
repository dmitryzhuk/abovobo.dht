package org.abovobo.dht

import org.abovobo.dht.persistence.h2.{Reader, Writer}
import org.h2.tools.RunScript
import java.sql.DriverManager
import java.io.FileReader
import java.io.File

object DhtDbCreator extends App {  
  
  def create(path: String) = {
	Class.forName("org.h2.Driver")
	val dbFile = new File((if (path.startsWith("~")) System.getProperty("user.home") + path.substring(1) else path) + ".h2.db")
	if (dbFile.delete()) {
	  println("deleted existing " + dbFile)
	}
	val connection = DriverManager.getConnection("jdbc:h2:" + path)
	RunScript.execute(connection, new FileReader("tables.sql"))
	println("db created at " + dbFile)
  }
  
  create("~/db/dht")
}
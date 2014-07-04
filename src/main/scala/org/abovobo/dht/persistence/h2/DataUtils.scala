package org.abovobo.dht.persistence.h2

import java.sql.DriverManager
import org.h2.tools.RunScript
import java.io.File
import org.h2.jdbcx.JdbcConnectionPool
import java.io.FileNotFoundException

object DataUtils {
  def openDatabase(fileLocation: String, createIfNotExists: Boolean = false): DataSource = { 
    val dbFile = location2file(fileLocation)
    val schemaUrl = toSchemaUrl(fileLocation)
    
    if (!dbFile.exists) {
      if (createIfNotExists) {
        import org.abovobo.jdbc.Closer._
        DataSource // init h2 driver
        val url = "jdbc:h2:" + fileLocation
        using(new java.io.InputStreamReader(this.getClass.getResourceAsStream("/tables.sql"))) { script =>
          using(DriverManager.getConnection(url)) { connection =>
            RunScript.execute(connection, script)
          }
        }
      } else {
        throw new FileNotFoundException(fileLocation)
      }
    } 

    DataSource(schemaUrl) 
  }
  
  private def toSchemaUrl(fileLocation: String) = "jdbc:h2:" + fileLocation + ";SCHEMA=ipv4"

  private def location2file(fileLocation: String) = 
    new File((if (fileLocation.startsWith("~")) System.getProperty("user.home") + fileLocation.substring(1) else fileLocation).split(";")(0) + ".h2.db")
}
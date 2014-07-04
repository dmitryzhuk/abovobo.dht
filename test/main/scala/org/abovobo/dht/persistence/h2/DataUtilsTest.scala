package org.abovobo.dht.persistence.h2

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import java.io.FileNotFoundException

class DataUtilsTest extends WordSpec with Matchers with BeforeAndAfterAll {
  "DataUtils" when {
    "openDatabase() is called" must {
      "create database" in {
        val dataSource = DataUtils.openDatabase("~/db/db-test", true)
        dataSource.close()
      }
      "create database suitable for reading" in {
        val dataSource = DataUtils.openDatabase("~/db/db-test", true)
        val storage = new Storage(dataSource.connection)
        storage.close()
        dataSource.close()
      }
    }
    "openDatabase('url', false) is called" must {
      "throw an exception" in {
        a[FileNotFoundException] should be thrownBy DataUtils.openDatabase("~/db/db-test-new", false)
      }
    }
  }
  
}
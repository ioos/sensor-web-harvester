package com.axiomalaska.sos.harvester.app

import org.junit._

@Test
class H2Test {
  private val testDbUrl:String = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1"
    
  //Use a file based h2 database if you want to examine the contents of the database after testing
  //private val testDbUrl:String = "jdbc:h2:/tmp/testdb"

  private val dbManager:MetadataDatabaseManager = new MetadataDatabaseManager(testDbUrl)

  @Test
  def testInit(){
    dbManager.init
    
    //run again to make sure re-init works
    dbManager.init
  }
}
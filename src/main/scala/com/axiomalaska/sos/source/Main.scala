package com.axiomalaska.sos.source

import com.axiomalaska.sos.data.Location

object Main {
  def main(args: Array[String]) {

    if(args(0) == "-metadata"){
      updateMetadata(args.tail)
    }else if (args(0) == "-updatesos") {
    	updateSos(args.tail)
    }
    else{
      println("must be a -metadata or -updatesos call")
    }
  }
  
  private def updateMetadata(args:Array[String]){
      val databaseUrl = args(0)
      val databaseUsername = args(1)
      val databasePassword = args(2)
      val northLat = args(3).toDouble
      val southLat = args(4).toDouble
      val westLon = args(5).toDouble
      val eastLon = args(6).toDouble
      
      val southWestCorner = new Location(southLat, westLon)
      val northEastCorner = new Location(northLat, eastLon)
      val boundingBox = BoundingBox(southWestCorner, northEastCorner)
      val metadataDatabaseManager = new MetadataDatabaseManager(
          databaseUrl, databaseUsername, databasePassword, boundingBox)
      
      metadataDatabaseManager.update()
  }
  
  private def updateSos(args:Array[String]){
      val sosUrl = args(0)
      val databaseUrl = args(1)
      val databaseUsername = args(2)
      val databasePassword = args(3)
      
      println("SOS URL: " + sosUrl)
      println("Database URL: " + databaseUrl)
      println("Database Username: " + databaseUsername)
      println("Database Password: " + databasePassword)
      val sosManager = new SosSourcesManager(databaseUrl,
        databaseUsername, databasePassword, sosUrl);

      sosManager.updateSos();
  }
}

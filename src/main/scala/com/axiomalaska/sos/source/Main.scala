package com.axiomalaska.sos.source

import com.axiomalaska.sos.data.Location
import org.apache.log4j.Logger

object Main {

  def main(args: Array[String]) {
    val logger = Logger.getRootLogger()
    if (args(0) == "-metadata") {
      updateMetadata(args.tail, logger)
    } else if (args(0) == "-updatesos") {
      updateSos(args.tail, logger)
    } else {
      logger.info("Must be a -metadata [databaseUrl] [databaseUsername] [databasePassword] [North most latitude] [South most latitude] [West most longitude] [East most longitude] \n" + 
          "or -updatesos [SOS URL] [databaseUrl] [databaseUsername] [databasePassword]")
    }
  }
  
  private def updateMetadata(args:Array[String], logger: Logger){
    if(args.size == 7){
      val databaseUrl = args(0)
      val databaseUsername = args(1)
      val databasePassword = args(2)
      val northLat = args(3).toDouble
      val southLat = args(4).toDouble
      val westLon = args(5).toDouble
      val eastLon = args(6).toDouble
      
      logger.info("Database URL: " + databaseUrl)
      logger.info("Database Username: " + databaseUsername)
      logger.info("Database Password: " + databasePassword)
      
      logger.info("North Lat: " + northLat)
      logger.info("Sourth Lat: " + southLat)
      logger.info("West Lat: " + westLon)
      logger.info("East Lat: " + eastLon)
      
      val southWestCorner = new Location(southLat, westLon)
      val northEastCorner = new Location(northLat, eastLon)
      val boundingBox = BoundingBox(southWestCorner, northEastCorner)
      val metadataDatabaseManager = new MetadataDatabaseManager(
          databaseUrl, databaseUsername, databasePassword, boundingBox, logger)
      
      metadataDatabaseManager.update()
    } else {
      logger.error("Must have 8 arguments -metadata [databaseUrl] [databaseUsername] [databasePassword] [North most latitude] [South most latitude] [West most longitude] [East most longitude]")
    }
  }
  
  private def updateSos(args:Array[String], logger: Logger){
    if(args.size == 4){
      val sosUrl = args(0)
      val databaseUrl = args(1)
      val databaseUsername = args(2)
      val databasePassword = args(3)
      
      logger.info("SOS URL: " + sosUrl)
      logger.info("Database URL: " + databaseUrl)
      logger.info("Database Username: " + databaseUsername)
      logger.info("Database Password: " + databasePassword)
      val sosManager = new SosSourcesManager(databaseUrl,
        databaseUsername, databasePassword, sosUrl, logger);

      sosManager.updateSos();
    } else{
      logger.error("Must have 5 arguments -updatesos [SOS URL] [databaseUrl] [databaseUsername] [databasePassword]")
    }
  }
}

package com.axiomalaska.sos.source

import com.axiomalaska.sos.data.Location
import org.apache.log4j.Logger
import com.axiomalaska.sos.data.PublisherInfoImp
import java.io.File
import java.util.Calendar
import javax.naming.ConfigurationException
import org.apache.commons.configuration.PropertiesConfiguration

object Main {

  def main(args: Array[String]) {
    val logger = Logger.getRootLogger()
    var timerBegin: Calendar = null
    if (args.size == 3) {
      val timer = args(2)
      if (timer == "-timer" || timer == "-timer=true") {
        // set a timer to report the time it takes to run the harverster
        logger.info("USING: Starting Timer")
        timerBegin = Calendar.getInstance
      }
    }
    if (args.size >= 2) {
      val tag = args(0)
      val propertiesFilePath = args(1)

      val properties = createProperties(propertiesFilePath)
      if (tag == "-metadata") {
        updateMetadata(properties, logger)
      } else if (tag == "-updatesos") {
        updateSos(properties, logger)
      } else{
    	logger.info("Must be a -metadata [properties file] \n" + 
          "or -updatesos [properties file]")
      }
    }
    else{
    	logger.info("Must be a -metadata [properties file] \n" + 
          "or -updatesos [properties file]")
    }
    
    if (timerBegin != null) {
      val elapsedTime = (Calendar.getInstance.getTimeInMillis - timerBegin.getTimeInMillis) / 1000L
      logger.info("Total Elapsed Time for harvesting (in secs): " + elapsedTime)
   }
  }
  
//  def main(args: Array[String]) {
//    val queryBuilder = new StationQueryBuilder(
//        "jdbc:postgresql://localhost:5432/sensor", "sensoruser", "sensor")
//    
//    queryBuilder.withStationQuery(stationQuery => {
//      val stationUpdater = new HadsStationUpdater(stationQuery, BoundingBox(new Location(39.0, -80.0), 
//        new Location(40.0, -74.0)))
//      stationUpdater.update()
//    })
//  }
  
  private def updateMetadata(properties:PropertiesConfiguration, logger: Logger){
      val databaseUrl = properties.getString("database_url")
      val databaseUsername = properties.getString("database_username")
      val databasePassword = properties.getString("database_password")
      val northLat = properties.getDouble("north_lat")
      val southLat = properties.getDouble("south_lat")
      val westLon = properties.getDouble("west_lon")
      val eastLon = properties.getDouble("east_lon")
      var sources = "all"
      
      if (properties.containsKey("sources"))
        sources = properties.getString("sources")
      
      logger.info("Database URL: " + databaseUrl)
      logger.info("Database Username: " + databaseUsername)
      logger.info("Database Password: " + databasePassword)
      
      logger.info("North Lat: " + northLat)
      logger.info("South Lat: " + southLat)
      logger.info("West Lat: " + westLon)
      logger.info("East Lat: " + eastLon)
      
      logger.info("Sources Used: " + sources)
      
      val southWestCorner = new Location(southLat, westLon)
      val northEastCorner = new Location(northLat, eastLon)
      val boundingBox = BoundingBox(southWestCorner, northEastCorner)
      val metadataDatabaseManager = new MetadataDatabaseManager(
        databaseUrl, databaseUsername, databasePassword, boundingBox, sources.toLowerCase, logger)
      
      metadataDatabaseManager.update()
  }
  
  private def updateSos(properties:PropertiesConfiguration, logger: Logger){
      val sosUrl = properties.getString("sos_url")
      val databaseUrl = properties.getString("database_url")
      val databaseUsername = properties.getString("database_username")
      val databasePassword = properties.getString("database_password")
      val country = properties.getString("publisher_country", "country")
      val email = properties.getString("publisher_email", "email")
      val name = properties.getString("publisher_name", "name")
      val webAddress = properties.getString("publisher_web_address", "web_address")
      var sources: String = "all"
      
      if (properties.containsKey("sources"))
        sources = properties.getString("sources")
      
      logger.info("SOS URL: " + sosUrl)
      logger.info("Database URL: " + databaseUrl)
      logger.info("Database Username: " + databaseUsername)
      logger.info("Database Password: " + databasePassword)
      logger.info("country: " + country)
      logger.info("email: " + email)
      logger.info("name: " + name)
      logger.info("webAddress: " + webAddress)
      
      val publisherInfo = new PublisherInfoImp()
      publisherInfo.setCountry(country)
      publisherInfo.setEmail(email)
      publisherInfo.setName(name)
      publisherInfo.setWebAddress(webAddress)
      
      val sosManager = new SosSourcesManager(databaseUrl,
        databaseUsername, databasePassword, sosUrl, publisherInfo, sources, logger);

      sosManager.updateSos();
  }
  
  private def createProperties(propertiesFilePath: String): PropertiesConfiguration = {
    val propertiesFile = new File(propertiesFilePath);

    if (!propertiesFile.exists()) {
      throw new ConfigurationException(
        "No server file found. Pass server file path as Second argument");
    }

    val properties = new PropertiesConfiguration(propertiesFile);

    return properties;
  }
}

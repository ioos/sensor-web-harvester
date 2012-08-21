package com.axiomalaska.sos.source

import com.axiomalaska.sos.data.Location
import org.apache.log4j.Logger
import com.axiomalaska.sos.data.PublisherInfoImp
import java.io.File
import javax.naming.ConfigurationException
import org.apache.commons.configuration.PropertiesConfiguration

object Main {

  def main(args: Array[String]) {
    val logger = Logger.getRootLogger()
    if (args.size == 2) {
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
  }
  
  private def updateMetadata(properties:PropertiesConfiguration, logger: Logger){
      val databaseUrl = properties.getString("database_url")
      val databaseUsername = properties.getString("database_username")
      val databasePassword = properties.getString("database_password")
      val northLat = properties.getDouble("north_lat")
      val southLat = properties.getDouble("south_lat")
      val westLon = properties.getDouble("west_lon")
      val eastLon = properties.getDouble("east_lon")
      
      logger.info("Database URL: " + databaseUrl)
      logger.info("Database Username: " + databaseUsername)
      logger.info("Database Password: " + databasePassword)
      
      logger.info("North Lat: " + northLat)
      logger.info("South Lat: " + southLat)
      logger.info("West Lat: " + westLon)
      logger.info("East Lat: " + eastLon)
      
      val southWestCorner = new Location(southLat, westLon)
      val northEastCorner = new Location(northLat, eastLon)
      val boundingBox = BoundingBox(southWestCorner, northEastCorner)
      val metadataDatabaseManager = new MetadataDatabaseManager(
          databaseUrl, databaseUsername, databasePassword, boundingBox, logger)
      
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
        databaseUsername, databasePassword, sosUrl, publisherInfo, logger);

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

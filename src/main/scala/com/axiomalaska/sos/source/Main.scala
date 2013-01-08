package com.axiomalaska.sos.source

import com.axiomalaska.sos.data.Location
import org.apache.log4j.Logger
import com.axiomalaska.sos.data.PublisherInfoImp
import java.io.File
import java.util.Calendar
import javax.naming.ConfigurationException
import org.apache.commons.configuration.PropertiesConfiguration
import com.axiomalaska.sos.data.SosNetworkImp

case class Properties(val sosUrl: String,
      val country: String,
      val email: String,
      val name: String,
      val webAddress: String,
      val databaseUrl: String,
      val databaseUsername: String,
      val databasePassword: String,
      val northLat: Double,
      val southLat: Double,
      val westLon: Double,
      val eastLon: Double,
      val sources: String,
      val isoTemplate: String,
      val isoLocation: String,
      val rootNetworkId: String,
      val rootNetworkSourceId: String)

object Main {

  private var overWrite: Boolean = true
  
  def main(args: Array[String]) {
    val logger = Logger.getRootLogger()
    var timerBegin: Calendar = null
    if (args.size > 2) {
      for {
        (arg,index) <- args.zipWithIndex
        if (index > 1)
      } {
        if (arg.equalsIgnoreCase("-timer")) {
          logger info "USING: Timer"
          timerBegin = Calendar.getInstance
        } else if (arg.equalsIgnoreCase("-nooverwrite")) {
          // set a flag to not overwrite (when writing isos)
          logger info "USING: Will not overwrite existing files"
          overWrite = false
        }
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
      } else if (tag == "-writeiso") {
        writeISOFiles(properties, logger)
      } else{
    	logger.info("Must be a -metadata [properties file] \n" + 
          "or -updatesos [properties file]\n" + 
          "or -writeiso [properties file]")
      }
    }
    else{
    	logger.info("Must be a -metadata [properties file] \n" + 
          "or -updatesos [properties file]\n" + 
          "or -writeiso [properties file]")
    }
    
    if (timerBegin != null) {
      val elapsedTime = (Calendar.getInstance.getTimeInMillis - timerBegin.getTimeInMillis) / 1000L
      logger.info("Total Elapsed Time for harvesting (in secs): " + elapsedTime)
   }
  }
  
  private def updateMetadata(properties:PropertiesConfiguration, logger: Logger){
//      val databaseUrl = properties.getString("database_url")
//      val databaseUsername = properties.getString("database_username")
//      val databasePassword = properties.getString("database_password")
//      val northLat = properties.getDouble("north_lat")
//      val southLat = properties.getDouble("south_lat")
//      val westLon = properties.getDouble("west_lon")
//      val eastLon = properties.getDouble("east_lon")
//      var sources = "all"
//      
//      if (properties.containsKey("sources"))
//        sources = properties.getString("sources")
//        
    val propertiesRead = readProperties(properties)
      
    logger.info("Database URL: " + propertiesRead.databaseUrl)
    logger.info("Database Username: " + propertiesRead.databaseUsername)
    logger.info("Database Password: " + propertiesRead.databasePassword)

    logger.info("North Lat: " + propertiesRead.northLat)
    logger.info("South Lat: " + propertiesRead.southLat)
    logger.info("West Lon: " + propertiesRead.westLon)
    logger.info("East Lon: " + propertiesRead.eastLon)

    logger.info("Sources Used: " + propertiesRead.sources)

    val southWestCorner = new Location(propertiesRead.southLat, propertiesRead.westLon)
    val northEastCorner = new Location(propertiesRead.northLat, propertiesRead.eastLon)
    val boundingBox = BoundingBox(southWestCorner, northEastCorner)
    val metadataDatabaseManager = new MetadataDatabaseManager(
      propertiesRead.databaseUrl, propertiesRead.databaseUsername, propertiesRead.databasePassword,
      boundingBox, propertiesRead.sources.toLowerCase, logger)

    metadataDatabaseManager.update()
  }
  
  private def updateSos(properties:PropertiesConfiguration, logger: Logger){
//      val sosUrl = properties.getString("sos_url")
//      val databaseUrl = properties.getString("database_url")
//      val databaseUsername = properties.getString("database_username")
//      val databasePassword = properties.getString("database_password")
//      val country = properties.getString("publisher_country", "country")
//      val email = properties.getString("publisher_email", "email")
//      val name = properties.getString("publisher_name", "name")
//      val webAddress = properties.getString("publisher_web_address", "web_address")
//      var sources: String = "all"
//      
//      if (properties.containsKey("sources"))
//        sources = properties.getString("sources")
      
    val propertiesRead = readProperties(properties)
      
    logger.info("SOS URL: " + propertiesRead.sosUrl)
    logger.info("Database URL: " + propertiesRead.databaseUrl)
    logger.info("Database Username: " + propertiesRead.databaseUsername)
    logger.info("Database Password: " + propertiesRead.databasePassword)
    logger.info("country: " + propertiesRead.country)
    logger.info("email: " + propertiesRead.email)
    logger.info("name: " + propertiesRead.name)
    logger.info("webAddress: " + propertiesRead.webAddress)
    logger.info("Root Network Id: " + propertiesRead.rootNetworkId)
    logger.info("Root Network Source Id: " + propertiesRead.rootNetworkSourceId)

    val publisherInfo = new PublisherInfoImp()
    publisherInfo.setCountry(propertiesRead.country)
    publisherInfo.setEmail(propertiesRead.email)
    publisherInfo.setName(propertiesRead.name)
    publisherInfo.setWebAddress(propertiesRead.webAddress)

    val rootNetwork = new SosNetworkImp();
    rootNetwork.setId(propertiesRead.rootNetworkId);
    rootNetwork.setSourceId(propertiesRead.rootNetworkSourceId);

    val sosManager = new SosSourcesManager(propertiesRead.databaseUrl,
      propertiesRead.databaseUsername, propertiesRead.databasePassword, propertiesRead.sosUrl, publisherInfo, propertiesRead.sources,
      rootNetwork, logger);

    sosManager.updateSos();
  }
  
  private def writeISOFiles(properties: PropertiesConfiguration, logger: Logger) = {
    val propertiesRead = readProperties(properties)
    
    logger.info("Sources used: " + propertiesRead.sources)
    logger.info("ISO Template: " + propertiesRead.isoTemplate)
    logger.info("ISO Write Location: " + propertiesRead.isoLocation)
    logger.info("Database URL: " + propertiesRead.databaseUrl)
    logger.info("Database Username: " + propertiesRead.databaseUsername)
    logger.info("Database Password: " + propertiesRead.databasePassword)
    
    val isoManager = new ISOSourcesManager(propertiesRead.isoTemplate, propertiesRead.isoLocation, propertiesRead.sources, propertiesRead.databaseUrl,
                                           propertiesRead.databaseUsername, propertiesRead.databasePassword, overWrite, logger)
    
    isoManager.writeISOs()
  }
  
  private def readProperties(properties: PropertiesConfiguration) : Properties = {
    val sosUrl = properties.getString("sos_url")
    val databaseUrl = properties.getString("database_url")
    val databaseUsername = properties.getString("database_username")
    val databasePassword = properties.getString("database_password")
    val country = properties.getString("publisher_country", "country")
    val email = properties.getString("publisher_email", "email")
    val name = properties.getString("publisher_name", "name")
    val webAddress = properties.getString("publisher_web_address", "web_address")
    var sources: String = "all"
    val northLat = properties.getDouble("north_lat")
    val southLat = properties.getDouble("south_lat")
    val westLon = properties.getDouble("west_lon")
    val eastLon = properties.getDouble("east_lon")
    val isoTemplate = properties.getString("iso_template")
    val isoLocation = properties.getString("iso_write_location")
    val rootNetworkId = properties.getString("root_network")
    val rootNetworkSourceId = properties.getString("root_network_source")

    if (properties.containsKey("sources"))
      sources = properties.getString("sources")
      
    return new Properties(sosUrl,country,email,name,webAddress,databaseUrl,databaseUsername,
                          databasePassword,northLat,southLat,westLon,eastLon,sources,isoTemplate,
                          isoLocation,rootNetworkId,rootNetworkSourceId)

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

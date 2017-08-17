package com.axiomalaska.sos.harvester.app

import java.io.File
import java.util.Calendar

import scala.Array.canBuildFrom

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger

import com.axiomalaska.ioos.sos.GeomHelper
import com.axiomalaska.sos.data.PublisherInfo
import com.axiomalaska.sos.harvester.BoundingBox
import com.axiomalaska.sos.harvester.iso.ISOSourcesManager

import javax.naming.ConfigurationException

case class Properties(val sosUrl: String,
      val country: String,
      val email: String,
      val name: String,
      val webAddress: String,
      val databaseUrl: String,
      val northLat: Double,
      val southLat: Double,
      val westLon: Double,
      val eastLon: Double,
      val sources: String,
      val isoTemplate: String,
      val isoLocation: String
      )

object Main {

  private var overWrite: Boolean = true
  private val LOGGER = Logger.getLogger(getClass())
  
  def main(args: Array[String]) {
    var timerBegin: Calendar = null
    if (args.size > 2) {
      for {
        (arg,index) <- args.zipWithIndex
        if (index > 1)
      } {
        if (arg.equalsIgnoreCase("-timer")) {
          LOGGER info "USING: Timer"
          timerBegin = Calendar.getInstance
        } else if (arg.equalsIgnoreCase("-nooverwrite")) {
          // set a flag to not overwrite (when writing isos)
          LOGGER info "USING: Will not overwrite existing files"
          overWrite = false
        }
      }
    }
    if (args.size >= 2) {
      val tag = args(0)
      val propertiesFilePath = args(1)

      val properties = createProperties(propertiesFilePath)
      initDatabase(properties)

      if (tag == "-metadata") {
        updateMetadata(properties)
      } else if (tag == "-updatesos") {
        updateSos(properties)
      } else if (tag == "-writeiso") {
        writeISOFiles(properties)
      } else{
    	LOGGER.info("Must be a -metadata [properties file] \n" + 
          "or -updatesos [properties file]\n" + 
          "or -writeiso [properties file]")
      }
    }
    else{
    	LOGGER.info("Must be a -metadata [properties file] \n" + 
          "or -updatesos [properties file]\n" + 
          "or -writeiso [properties file]")
    }
    
    if (timerBegin != null) {
      val elapsedTime = (Calendar.getInstance.getTimeInMillis - timerBegin.getTimeInMillis) / 1000L
      LOGGER.info("Total Elapsed Time for harvesting (in secs): " + elapsedTime)
   }
  }

  private def initDatabase(properties:PropertiesConfiguration){
    val propertiesRead = readProperties(properties)
    
    LOGGER.info("Initializing Database: " + propertiesRead.databaseUrl)
    
    val metadataDatabaseManager = new MetadataDatabaseManager(propertiesRead.databaseUrl)

    metadataDatabaseManager.init()    
  }
  
  private def updateMetadata(properties:PropertiesConfiguration){   
    val propertiesRead = readProperties(properties)
      
    LOGGER.info("Database URL: " + propertiesRead.databaseUrl)

    LOGGER.info("North Lat: " + propertiesRead.northLat)
    LOGGER.info("South Lat: " + propertiesRead.southLat)
    LOGGER.info("West Lon: " + propertiesRead.westLon)
    LOGGER.info("East Lon: " + propertiesRead.eastLon)

    LOGGER.info("Sources Used: " + propertiesRead.sources)

    
    val southWestCorner = GeomHelper.createLatLngPoint(propertiesRead.southLat, 
        propertiesRead.westLon)
    val northEastCorner = GeomHelper.createLatLngPoint(propertiesRead.northLat, 
        propertiesRead.eastLon)
    val boundingBox = BoundingBox(southWestCorner, northEastCorner)
    val metadataDatabaseManager = new MetadataDatabaseManager(propertiesRead.databaseUrl)

    metadataDatabaseManager.update(boundingBox, propertiesRead.sources.toLowerCase)
  }
  
  private def updateSos(properties:PropertiesConfiguration){
    val propertiesRead = readProperties(properties)

    val publisherInfo = new PublisherInfo()
    publisherInfo.setCountry(propertiesRead.country)
    publisherInfo.setEmail(propertiesRead.email)
    publisherInfo.setName(propertiesRead.name)
    publisherInfo.setWebAddress(propertiesRead.webAddress)
    publisherInfo.setCode(propertiesRead.name)

    val sosManager = new SosSourcesManager(propertiesRead.databaseUrl,
        propertiesRead.sosUrl, publisherInfo, propertiesRead.sources)

    sosManager.updateSos();
  }
  
  private def writeISOFiles(properties: PropertiesConfiguration) = {
    val propertiesRead = readProperties(properties)
    
    val publisherInfo = new PublisherInfo()
    publisherInfo.setCountry(propertiesRead.country)
    publisherInfo.setEmail(propertiesRead.email)
    publisherInfo.setName(propertiesRead.name)
    publisherInfo.setWebAddress(propertiesRead.webAddress)
    
    val isoManager = new ISOSourcesManager(propertiesRead.isoTemplate, 
        propertiesRead.isoLocation, propertiesRead.sources, propertiesRead.databaseUrl, 
        overWrite, publisherInfo)
    
    isoManager.writeISOs()
  }
  
  private def readProperties(properties: PropertiesConfiguration) : Properties = {
    val sosUrl = properties.getString("sos_url")
    val databaseUrl = properties.getString("database_url")
    val country = properties.getString("publisher_country", "country")
    val email = properties.getString("publisher_email", "email")
    val name = properties.getString("publisher_name", "name")
    val webAddress = properties.getString("publisher_web_address", "web_address")
    val sources = properties.getString("sources", "all")
    val northLat = properties.getDouble("north_lat")
    val southLat = properties.getDouble("south_lat")
    val westLon = properties.getDouble("west_lon")
    val eastLon = properties.getDouble("east_lon")
    val isoTemplate = properties.getString("iso_template")
    val isoLocation = properties.getString("iso_write_location")
      
    LOGGER.info("sosUrl: " + sosUrl)
    LOGGER.info("databaseUrl: " + databaseUrl)
    LOGGER.info("country: " + country)
    LOGGER.info("email: " + email)
    LOGGER.info("name: " + name)
    LOGGER.info("webAddress: " + webAddress)
    LOGGER.info("sources: " + sources)
    LOGGER.info("northLat: " + northLat)
    LOGGER.info("southLat: " + southLat)
    LOGGER.info("westLon: " + westLon)
    LOGGER.info("eastLon: " + eastLon)
    LOGGER.info("isoTemplate: " + isoTemplate)
    LOGGER.info("isoLocation: " + isoLocation)
    
    Properties(sosUrl,country,email,name,webAddress,databaseUrl,
        northLat,southLat,westLon,eastLon,sources,isoTemplate,isoLocation)
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

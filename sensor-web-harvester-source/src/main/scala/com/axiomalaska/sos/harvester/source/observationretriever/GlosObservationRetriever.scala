/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.harvester.source.observationretriever

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

import scala.Array.canBuildFrom

import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.net.ftp.FTPFile
import org.apache.commons.net.ftp.FTPReply
import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatterBuilder
import org.joda.time.format.DateTimeParser

import com.axiomalaska.sos.harvester.StationQuery
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.ObservationValues
import com.axiomalaska.sos.harvester.data.SourceId
import com.axiomalaska.sos.tools.HttpSender
import java.io.File
import com.typesafe.config.ConfigFactory

object GlosObservationRetriever {
  private var filesInMemory: List[String] = Nil
  private var currentStationName: String = ""
}

class GlosObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger())
	extends ObservationValuesCollectionRetriever {
import GlosObservationRetriever._  
  
  private val httpSender = new HttpSender()

  private val dateParser = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")
  private val dateParserZone = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss z")
  private val parserArray: Array[DateTimeParser] = Array(dateParser.getParser(), dateParserZone.getParser())
  private val dateFormatter = new DateTimeFormatterBuilder().append(null, parserArray).toFormatter()
            
  private val source = stationQuery.getSource(SourceId.GLOS)
  private val stationList = stationQuery.getAllStations(source)
  
  private val MAX_FILE_LIMIT = 2500
  
  private var filesToMove: List[String] = List()
    
  //ftp info
  private val config = ConfigFactory.parseFile(new File("glos.conf"))
  /*
  glos.conf file needed with ftp connection config in same dir as sensor-web-harvester jar, example:

  ftp {
    host = "glos.us"
    port = 21
    user = "someuser"
    pass = "somepassword"
  }
  */
  private val ftp_host = config.getString("ftp.host")
  private val ftp_port = config.getInt("ftp.port")
  private val ftp_user = config.getString("ftp.user")
  private val ftp_pass = config.getString("ftp.pass")

  private val reloc_dir = "processed"
  private val glos_ftp: FTPClient = new FTPClient()
  private var fileList: Array[FTPFile] = null
    
  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
    phenomenon: LocalPhenomenon, startDate: DateTime):List[ObservationValues] = {

    logger.info("GLOS: Collecting for station - " + station.databaseStation.foreign_tag)
    
    val stid = if (station.getId.contains(":")) station.getId.substring(station.getId.lastIndexOf(":") + 1) else station.getId
    
    // retrieve files if needed
    if (!currentStationName.equals(stid)) {
      readInFtpFilesIntoMemory(stid)
      currentStationName = stid
    }
    val observationValuesCollection = createSensorObservationValuesCollection(station, sensor, phenomenon)
    
    // iterate through the files on the server, match their station text to the station name
    // then get date of the file and check it against the startDate
    // finally iterate over the observation values and match the tags to what the file has, adding data to that observationValue
    val sttag = if (station.databaseStation.foreign_tag.contains(":")) station.databaseStation.foreign_tag.substring(
        station.databaseStation.foreign_tag.lastIndexOf(":")+1) else station.databaseStation.foreign_tag
    for (matchedFile <- filesInMemory.filter(_.contains("<station>" + sttag + "</station>"))) {
      val stationXML = scala.xml.XML.loadString(matchedFile)
      for {
        message <- (stationXML \\ "message")
        reportDate = {
          val dateStr = (message \ "date").text.replace("UTC","GMT") // joda time only recognizes "GMT" as the timezone, not "UTC"
          try {
            dateFormatter.parseDateTime(dateStr)
          } catch {
            case ex: Exception => logger.error("Exception reading in file: " + ex.toString + ".  Can't parse date format: " + dateStr + "!")
            null
          }
        }
        if(!reportDate.eq(null) && reportDate.isAfter(startDate))
      } {
        for (observation <- observationValuesCollection) {
          val tag = observation.observedProperty.foreign_tag
          (message \\ tag).filter(p => p.text != "").foreach(f => {
              try {
                observation.addValue(f.text.trim.toDouble, reportDate)
              } catch {
                case ex: Exception => logger.error(ex.toString)
              }
            })
        }
      }
    }
    
    observationValuesCollection.filter(_.getValues.size > 0)
  }
  
  private def readInFtpFilesIntoMemory(stationId: String) = {
    logger.info("FTP host: " + ftp_host)
    logger.info("FTP port: " + ftp_port)
    logger.info("FTP user: " + ftp_user)

    try {
      if (!glos_ftp.isConnected) {
        glos_ftp.connect(ftp_host, ftp_port)
        glos_ftp.login(ftp_user, ftp_pass)
        if(!FTPReply.isPositiveCompletion(glos_ftp.getReplyCode)) {
          glos_ftp.disconnect
          logger.error("FTP connection was refused.")
        } else {
          glos_ftp.enterLocalPassiveMode
          glos_ftp.setControlKeepAliveTimeout(60)
          glos_ftp.setDataTimeout(60000)

          // grab list of files all at once on connection
          fileList = glos_ftp.listFiles
        }
      }
    } catch {
      case ex: Exception => {
          logger.error("Error connecting to ftp\n" + ex.toString)
          if (glos_ftp.isConnected)
            glos_ftp.disconnect
        }
    }
    
    try {
      filesInMemory = Nil
      filesToMove = Nil
      var fileCount = 0
      val files = for {
        file <- fileList
        if (file.getName.toLowerCase.contains(stationId.toLowerCase) && fileCount < MAX_FILE_LIMIT)
      } yield {
        fileCount += 1
        logger.info("Reading file [" + file.getName + "]: " + fileCount + " out of " + fileList.size)
        readFileIntoString(file.getName)
      }
      filesInMemory = files.filter(_.isDefined).map(m => m.get).toList
    } catch {
      case ex: Exception => logger.error("Exception reading in file: " + ex.toString)
    }
    
    try {
      logger.info("moving files to processed folder")
      for (file <- filesToMove) {
         logger.info("Moving file " + file)
         moveFileToProcessed(file)
      }
    } catch {
      case ex: Exception => logger.error("Exception removing files from ftp")
    }
    
    try {
      logger.info("disconnecting gftp")
      if (glos_ftp.isConnected)
        glos_ftp.disconnect
    } catch {
      case ex: Exception => logger.error("Exception closing ftp " + ex.toString)
    }
  }

  private def createSensorObservationValuesCollection(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    for (observedProperty <- observedProperties) yield {
      new ObservationValues(observedProperty, sensor, phenomenon, observedProperty.foreign_units)
    }
  }

  private def moveFileToProcessed(fileName: String) = {
    val dest = "./" + reloc_dir + "/" + fileName
    var success = false
    var attempts = 0
    while (attempts < 3 && !success) {
      try {
        success = glos_ftp.rename(fileName, dest)
        attempts += 1
      } catch {
        case ex: Exception => {
            logger.error("ERROR renaming file on server: " + fileName)
            success = false
            attempts = 3
        }
      }
    }
    
    if (success)
      logger.info("Successfully moved file " + fileName + " to " + dest)
    else
      logger.warn("Unable to move file " + fileName + " after three attempts.")
  }    
  
  private def readFileIntoString(fileName : String) : Option[String] = {
    val byteStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    var inpStream: ByteArrayInputStream = null
    var success = false
    var attempts = 0
    var retval: Option[String] = None
    while (attempts < 3 && !success) {
      attempts += 1
      try {
        glos_ftp.retrieveFile(fileName, byteStream) match {
          case true => {
              filesToMove = fileName :: filesToMove
              inpStream = new ByteArrayInputStream(byteStream.toByteArray)

              val string = byteStream.toString
              retval = new Some[String](string)
          }
          case false => {
              logger.error("could not read in file " + fileName)
              retval = None
          }
        }
        success = true
      } catch {
        case ex: Exception => {
            logger.warn("Attempt " + attempts + " failed to read file " + fileName)
        }
      }
    }
    if(!success)
      logger.error("Could not read in file " + fileName + " after 3 attempts.")
    retval
  }
}
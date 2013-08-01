/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.source.data.ObservationValues
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.tools.HttpSender
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.apache.commons.net.ftp.{FTPFile, FTPClient, FTPReply}
import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object GlosObservationRetriever {
  private var filesInMemory: List[scala.xml.Elem] = Nil
  private var currentStationName: String = ""
}

class GlosObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger())
	extends ObservationValuesCollectionRetriever {
  import GlosObservationRetriever._  
  
  private val httpSender = new HttpSender()
  private val dateParser = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")
            
  private val source = stationQuery.getSource(SourceId.GLOS)
  private val stationList = stationQuery.getAllStations(source)
  
  private val MAX_FILE_LIMIT = 2500
  
  private var filesToMove: List[String] = List()
    
  //ftp info - below is the glos server
  private val ftp_host = "glos.us"
  private val ftp_port = 21
  private val ftp_user = "asa"
  private val ftp_pass = "AGSLaos001"
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
    val stationXMLList = getMatchedStations(sttag)
    for (stationXML <- stationXMLList) {
      for {
        message <- (stationXML \\ "message")
        val reportDate = {
          val dateStr = (message \ "date").text
          dateParser.parseDateTime(dateStr)
        }
        if(reportDate.isAfter(startDate))
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
        readFileIntoXML(file.getName)
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
  
  private def getMatchedStations(stationTag : String) : List[scala.xml.Elem] = {
    val xmlList = for {
      file <- filesInMemory
    } yield {
      if (file != null) {
        if ((file \\ "message" \ "station").head.text.trim.equalsIgnoreCase(stationTag)) {
          new Some[scala.xml.Elem](file)
        } else {
          None
        }
      } else {
        logger.error("Removing file from memory")
        filesInMemory = { filesInMemory diff List(file) }
        None
      }
    }
    xmlList.filter(_.isDefined).map(g => g.get).toList
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
  
  private def readFileIntoXML(fileName : String) : Option[scala.xml.Elem] = {
    val byteStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    var inpStream: ByteArrayInputStream = null
    var success = false
    var attempts = 0
    var retval: Option[scala.xml.Elem] = None
    while (attempts < 3 && !success) {
      attempts += 1
      try {
        glos_ftp.retrieveFile(fileName, byteStream) match {
          case true => {
              filesToMove = fileName :: filesToMove
              inpStream = new ByteArrayInputStream(byteStream.toByteArray)

              val xml = scala.xml.XML.load(inpStream)
              retval = new Some[scala.xml.Elem](xml)
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
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
import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.net.ftp.FTPReply
import org.apache.log4j.Logger

object GlosObservationRetriever {
  private var filesInMemory: List[scala.xml.Elem] = Nil
}

class GlosObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger())
	extends ObservationValuesCollectionRetriever {
  import GlosObservationRetriever._  
  
  private val httpSender = new HttpSender()
  private val dateParser = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
  
  private val source = stationQuery.getSource(SourceId.GLOS)
  private val stationList = stationQuery.getAllStations(source)
  
  private val MAX_FILE_LIMIT = 200
  
  private var filesToRemove: List[String] = List()
    
  //ftp info - below is the glos server
//  private val ftp_host = "glos.us"
//  private val ftp_port = 21
//  private val ftp_user = "asa"
//  private val ftp_pass = "AGSLaos001"
  // below is a temp server used for testing
  private val ftp_host = "ftp.oilmap.com"
  private val ftp_port = 21
  private val ftp_user = "scowan"
  private val ftp_pass = "KQ6T6m1B"
  private val glos_ftp: FTPClient = new FTPClient()  
    
  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
    phenomenon: LocalPhenomenon, startDate: Calendar):List[ObservationValues] = {

    logger.info("GLOS: Collecting for station - " + station.databaseStation.foreign_tag)
    
    // retrieve files if needed
    if (filesInMemory.size < 1)
      readInFtpFilesIntoMemory
    
    val observationValuesCollection = createSensorObservationValuesCollection(station, sensor, phenomenon)
    
    // iterate through the files on the server, match their station text to the station name
    // then get date of the file and check it against the startDate
    // finally iterate over the observation values and match the tags to what the file has, adding data to that observationValue
    val stationXMLList = getMatchedStations(station.databaseStation.foreign_tag)
    for (stationXML <- stationXMLList) {
      for {
        message <- (stationXML \\ "message")
        val reportDate = {
          val dateStr = (message \ "date").text
          val date = dateParser.parse(dateStr)
          val calendar = Calendar.getInstance
          calendar.setTimeInMillis(date.getTime)
          calendar.getTime
          calendar
        }
        if(reportDate.after(startDate))
      } {
        for (observation <- observationValuesCollection) {
          val tag = observation.observedProperty.foreign_tag
          (message \\ tag).filter(p => p.text != "").foreach(f => observation.addValue(f.text.trim.toDouble, reportDate))
        }
      }
    }
    
    observationValuesCollection.filter(_.getValues.size > 0)
  }
  
  private def readInFtpFilesIntoMemory() = {
    // connect to the ftp
    try {
      if (!glos_ftp.isConnected) {
        glos_ftp.connect(ftp_host, ftp_port)
        glos_ftp.login(ftp_user, ftp_pass)
        // check for succesful login
        if(!FTPReply.isPositiveCompletion(glos_ftp.getReplyCode)) {
          glos_ftp.disconnect
          logger.error("FTP connection was refused.")
        } else {
          // set to passive mode
          glos_ftp.enterLocalPassiveMode
          // set timeouts to 1 min
          glos_ftp.setControlKeepAliveTimeout(60)
          glos_ftp.setDataTimeout(60000)
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
      // get file list; only taking the first MAX_FILE_LIMIT items
      val fileList = glos_ftp.listFiles.take(MAX_FILE_LIMIT)
      // read files and add their contents to the file list in memory
      var fileCount = 0
      val files = for {
        file <- fileList
      } yield {
        fileCount += 1
        logger.info("\nReading file [" + file.getName + "]: " + fileCount + " out of " + fileList.size)
        readFileIntoXML(file.getName)
      }
      // get a list with nones removed
      filesInMemory = files.filter(_.isDefined).map(m => m.get).toList
    } catch {
      case ex: Exception => logger.error("Exception reading in file: " + ex.toString)
    }
    
    // remove files from ftp
    try {
      logger.info("removing files that have been read into memory")
      for (file <- filesToRemove) {
        removeFileOnServer(file)
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
    logger.info("checking for station tag " + stationTag)
    val xmlList = for {
      file <- filesInMemory
    } yield {
      if (file != null) {
        if ((file \\ "message" \ "station").text.trim.equalsIgnoreCase(stationTag)) {
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
  
  private def removeFileOnServer(fileName: String) = {
    var success = false
    var attempts = 0
    while (attempts < 3 && !success) {
      try {
        success = glos_ftp.deleteFile(fileName)
        attempts += 1
      } catch {
        case ex: Exception => {
            logger.error("ERROR removinjavag file from server: " + fileName)
            success = false
            attempts = 3
        }
      }
    }
    
    if (success)
      logger.info("Successfully deleted file " + fileName + " from server.")
    else
      logger.info("Unable to delte file " + fileName + " after three attempts.")
  }
  
  private def readFileIntoXML(fileName : String) : Option[scala.xml.Elem] = {
    val byteStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    var success = false
    var attempts = 0
    var retval: Option[scala.xml.Elem] = None
    while (attempts < 3 && !success) {
      attempts += 1
      try {
        glos_ftp.retrieveFile(fileName, byteStream) match {
          case true => {
              logger.info("got a positive on file received")
              filesToRemove = fileName :: filesToRemove
              // doing a test load of xml to throw exception in case file wasn't fully downloaded
              val xml = scala.xml.XML.loadString(byteStream.toString("UTF-8").trim)
              retval = new Some[scala.xml.Elem](xml)
          }
          case false => {
              logger.info("could not read in file " + fileName)
              retval = None
          }
        }
        success = true
      } catch {
        case ex: Exception => {
            logger.info("Attempt " + attempts + " failed to read file " + fileName)
        }
      }
    }
    if(!success)
      logger.error("Could not read in file " + fileName + " after 3 attempts.")
    retval
  }
}
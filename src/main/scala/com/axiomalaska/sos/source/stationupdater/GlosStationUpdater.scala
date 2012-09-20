/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.tools.HttpSender
import org.apache.commons.net.ftp.FTPReply
import org.apache.log4j.Logger
import java.io.ByteArrayOutputStream
import org.apache.commons.net.ftp.FTPClient

case class GlosStation (stationName: String, stationId: String, stationDesc: String, lat: Double, lon: Double)

class GlosStationUpdater (private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  val name = "GLOS"
  
  //ftp info
  private val ftp_host = "glos.us"
  private val ftp_port = 21
  private val ftp_user = "asa"
  private val ftp_pass = "AGSLaos001"
  private val glos_ftp: FTPClient = new FTPClient()
  
  private val temp_file = "platform.csv"

  private val source = stationQuery.getSource(SourceId.GLOS)
  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val httpSender = new HttpSender()
  private var stationList: List[String] = Nil
  
  def update() {
    
    val sourceStationSensors = getSourceStations()

    val databaseStations = stationQuery.getStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
    
  }
  
  private def getSourceStations() : List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    // read in csv file and parse each line into a station to read in
    val file = scala.io.Source.fromFile(temp_file)
    val stationCollection = for {
      line <- file.getLines
      if(!line.contains("row_id"))
      val station = readStationFromLine(line)
      val dbStation = new DatabaseStation(station.stationName, station.stationId, station.stationId, station.stationDesc, "BUOY", SourceId.GLOS, station.lat, station.lon)
      val sourceObservedProperties = getObservedProperties
      val databaseObservedProperties = stationUpdater.updateObservedProperties(source, sourceObservedProperties)
      val sensors = stationUpdater.getSourceSensors(dbStation, databaseObservedProperties)
      if (sensors.nonEmpty)
    } yield {
      (dbStation, sensors)
    }
    if (stationCollection.nonEmpty) {
      stationCollection.toList
    }
    else {
      logger.info("Empty station collection!")
      Nil
    }
  }
  
  private def getObservedProperties() : List[ObservedProperty] = {
    List(new ObservedProperty("wdir1",SourceId.GLOS,"deg",98),
        new ObservedProperty("wspd1",SourceId.GLOS,"m/s", 99),
        new ObservedProperty("gust1",SourceId.GLOS,"m/s", 100),
        new ObservedProperty("atmp1",SourceId.GLOS,"C", 101),
        new ObservedProperty("wtmp1",SourceId.GLOS,"C", 102),
        new ObservedProperty("rh1",SourceId.GLOS,"%", 103),
        new ObservedProperty("dewpt1",SourceId.GLOS,"C", 104),
        new ObservedProperty("barol",SourceId.GLOS,"atm", 105),
        new ObservedProperty("wvhgt",SourceId.GLOS,"m", 106),
        new ObservedProperty("dompd",SourceId.GLOS,"s", 107),
        new ObservedProperty("mwdir",SourceId.GLOS,"deg", 108),
        new ObservedProperty("dp001",SourceId.GLOS,"m", 109),
        new ObservedProperty("tp001",SourceId.GLOS,"C", 110),
        new ObservedProperty("dp002",SourceId.GLOS,"m", 109),
        new ObservedProperty("tp002",SourceId.GLOS,"C", 110),
        new ObservedProperty("dp003",SourceId.GLOS,"m", 109),
        new ObservedProperty("tp003",SourceId.GLOS,"C", 110))
  }
  
  private def readStationFromLine(line: String) : GlosStation = {
    val brokenLine = line.split("\",\"").map(_.replace("\"", ""))
    logger.info("line:\n" + line)
    var stationId = brokenLine(6)
    if (stationId == """\N""") {
      val stationBreak = brokenLine(16).split("=")
      logger.info("Breaking url:")
      for(break <- stationBreak) {
        logger.info(break)
      }
      if (stationBreak.size > 1)
        stationId = stationBreak(1)
      else {
        stationId = brokenLine(28)
        if (stationId == """\N""")
          stationId = brokenLine(14)
      }
    }
    var lat = 0d
    var lon = 0d
    if (brokenLine(7) == """\N""")
      lon = -9999d
    else
      lon = brokenLine(7).toDouble
    if (brokenLine(8) == """\N""")
      lat = -9999d
    else
      lat = brokenLine(8).toDouble
    logger.info("Populating station: " + brokenLine(14) + ", " + stationId + ", " + brokenLine(15) + ", " + lat + ", " + lon)
    new GlosStation(brokenLine(14), stationId, brokenLine(15), lat, lon)
  }
  
//  def update() {
//    try {
//      logger.info("Attempting to login to ftp")
//      glos_ftp.connect(ftp_host, ftp_port)
//      glos_ftp.login(ftp_user, ftp_pass)
//    } catch {
//      case e: Exception => logger.info("Exception in connecting to ftp: " + e.getMessage)
//    }
//    glos_ftp.enterLocalPassiveMode
//    logger.info("Reply code: " + glos_ftp.getReplyCode)
//    if(!FTPReply.isPositiveCompletion(glos_ftp.getReplyCode)) {
//      glos_ftp.disconnect
//      logger.error("FTP connection was refused.")
//      return
//    }
//    
//    val sourceStationSensors = getSourceStations()
////
////    val databaseStations = stationQuery.getStations(source)
////
////    stationUpdater.updateStations(sourceStationSensors, databaseStations)
////    
//    if (glos_ftp.isConnected) {
//      glos_ftp.disconnect
//    }
//  }
//  
//  private def getSourceStations() : List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
//    // read the ftp files to get the list of stations for updating
//    val fileList = glos_ftp.listFiles
//    val glosStations = for {
//      ftpFile <- fileList
//      if(!stationList.exists(p => p == ftpFile.getName))
//      val fileString = readStationFile(ftpFile.getName)
//      if (fileString.isDefined)
//    } yield {
//      
//    }
//    Nil
//  }
//  
//  private def readStationFile(fileName: String) : Option[String] = {
//    // create output stream to handle reading in file
//    val byteStream: ByteArrayOutputStream = new ByteArrayOutputStream()
//    glos_ftp.retrieveFile(fileName, byteStream) match {
//      case true => {
//          // read in file
//          new Some[String](byteStream.toString("UTF-8").trim)
//      }
//      case false => {
//          logger.info("error reading file " + fileName)
//          None
//      }
//    }
//  }
//  
//  private def readInStation(file: String) : Option[GlosStation] = {
//    val xml = scala.xml.XML.loadString(file)
//    val stationName = (xml \ "message" \ "station").text.trim
//    val lat = (xml \ "message" \ "date" \ "lat")
//    None
//  }
}
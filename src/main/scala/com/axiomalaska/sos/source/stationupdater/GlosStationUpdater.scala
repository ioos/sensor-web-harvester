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

case class GlosStation (stationName: String, lat: Double, lon: Double)

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

  private val source = stationQuery.getSource(SourceId.STORET)
  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val httpSender = new HttpSender()
  private var stationList: List[String] = Nil
  
  def update() {
    try {
      logger.info("Attempting to login to ftp")
      glos_ftp.connect(ftp_host, ftp_port)
      glos_ftp.login(ftp_user, ftp_pass)
    } catch {
      case e: Exception => logger.info("Exception in connecting to ftp: " + e.getMessage)
    }
    glos_ftp.enterLocalPassiveMode
    logger.info("Reply code: " + glos_ftp.getReplyCode)
    if(!FTPReply.isPositiveCompletion(glos_ftp.getReplyCode)) {
      glos_ftp.disconnect
      logger.error("FTP connection was refused.")
      return
    }
    
    val sourceStationSensors = getSourceStations()
//
//    val databaseStations = stationQuery.getStations(source)
//
//    stationUpdater.updateStations(sourceStationSensors, databaseStations)
//    
    if (glos_ftp.isConnected) {
      glos_ftp.disconnect
    }
  }
  
  private def getSourceStations() : List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    // read the ftp files to get the list of stations for updating
    val fileList = glos_ftp.listFiles
    val glosStations = for {
      ftpFile <- fileList
      if(!stationList.exists(p => p == ftpFile.getName))
      val fileString = readStationFile(ftpFile.getName)
      if (fileString.isDefined)
    } yield {
      
    }
    Nil
  }
  
  private def readStationFile(fileName: String) : Option[String] = {
    // create output stream to handle reading in file
    val byteStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    glos_ftp.retrieveFile(fileName, byteStream) match {
      case true => {
          // read in file
          new Some[String](byteStream.toString("UTF-8").trim)
      }
      case false => {
          logger.info("error reading file " + fileName)
          None
      }
    }
  }
  
  private def readInStation(file: String) : Option[GlosStation] = {
    val xml = scala.xml.XML.loadString(file)
    val stationName = (xml \ "message" \ "station").text.trim
    val lat = (xml \ "message" \ "date" \ "lat")
    None
  }
}
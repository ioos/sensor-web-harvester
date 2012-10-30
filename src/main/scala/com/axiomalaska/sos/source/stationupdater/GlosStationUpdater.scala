/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.tools.HttpSender
import org.apache.log4j.Logger
import org.apache.commons.net.ftp.FTPClient
import scala.collection.mutable

//case class GlosStation (stationName: String, stationId: String, stationDesc: String, lat: Double, lon: Double)

case class GLOSStation (stationName: String, stationId: String, stationDesc: String, platformType: String, lat: Double, lon: Double)

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
  private val glos_metadata_file = "glos_metadata_test.xml"

  private val source = stationQuery.getSource(SourceId.GLOS)
  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val httpSender = new HttpSender()
  private var stationList: List[String] = Nil
  
  private var phenomenaList = stationQuery.getPhenomena
  
  def update() {
    
    logger.info("Updating GLOS...")
    
    val sourceStationSensors = getSourceStations()

    val databaseStations = stationQuery.getStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
    
    logger.info("Finished updating GLOS")
    
  }
  
  private def getSourceStations() : List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    val xmlList = readInMetadata
    val finallist = for (xml <- xmlList) yield {
      val listitem = for {
        stationXML <- (xml \\ "Station")
        val station = readStationFromXML(stationXML)
        val stationDB = new DatabaseStation(station.stationName, station.stationId, station.stationId, station.stationDesc, station.platformType, SourceId.GLOS, station.lat, station.lon)
        val sensors = readInSensors(stationXML, stationDB)
        if (sensors.nonEmpty)
      } yield {
        (stationDB, sensors)
      }
      listitem
    }
    finallist.flatten.toList
  }
  
  private def readInSensors(stationxml : scala.xml.Node, station : DatabaseStation) : List[(DatabaseSensor, List[(DatabasePhenomenon)])] = {
    val sensors = for {
      sensor <- (stationxml \ "Sensors" \\ "Sensor")
      val sensorid = (sensor \\ "id").text.trim
      val sensordesc = (sensor \\ "description").text.trim
      val dbsensor = new DatabaseSensor(sensorid, sensordesc.toString, station.id)
      val phenomena = for {
        phenomenon <- (sensor \ "Phenomena" \\ "Phenomenon")
      } yield {
        val phenomList = phenomenaList.filter(p => p.tag == (phenomenon \ "tag").text.trim)
        if (phenomList.isEmpty)
          (addPhenomenon(phenomenon), (phenomenon \ "foreigntag").text.trim)
        else
          (phenomList.head, (phenomenon \ "foreigntag").text.trim)
      }
      val observedProperties = getObservedProperties(phenomena.toList)
      val dbObservedProperties = stationUpdater.updateObservedProperties(source, observedProperties)
    } yield {
        val sensorPhenomena = phenomena.map{case(p,s) => p}
        (dbsensor, sensorPhenomena.toList)
    }
    val phenomSensors = for {
      phenomSensor <- (stationxml \ "Sensors" \ "Phenomenon")
      val sensorid = (phenomSensor \ "foreigntag").text.trim
      val sensordesc = (phenomSensor \ "name").text.trim
      val dbsensor = new DatabaseSensor(sensorid, sensordesc, station.id)
      val phenomena = {
        val list = phenomenaList.filter(p => p.tag == (phenomSensor \ "tag").text.trim)
        if (list.isEmpty)
          List((addPhenomenon(phenomSensor), sensorid))
        else
          List((list.head, sensorid))
      }
      val observedProperties = getObservedProperties(phenomena)
      val dbObservedProperties = stationUpdater.updateObservedProperties(source, observedProperties)
    } yield {
      logger.info("looking at: " + (phenomSensor).text)
      val sensorPhenomenon = phenomena.map{case(p,s) => p}
      (dbsensor, sensorPhenomenon.toList)
    }
    sensors.toList ::: phenomSensors.toList
  }
  
  private def getObservedProperties(phenomena : List[(DatabasePhenomenon, String)]) : List[ObservedProperty] = {
    val obproplist = for((phenomenon, foreigntag) <- phenomena) yield {
      new ObservedProperty(foreigntag, SourceId.GLOS, phenomenon.units, phenomenon.id)
    }
    obproplist.toList
  }
  
  private def addPhenomenon(phenomxml : scala.xml.Node) : DatabasePhenomenon = {
    val tag = (phenomxml \ "tag").text.trim
    val units = (phenomxml \ "units").text.trim
    val name = (phenomxml \ "name").text.trim
    val desc = (phenomxml \ "description").text.trim
    var retval = new DatabasePhenomenon(tag, units, desc, name)
    retval = stationQuery.createPhenomenon(retval)
    phenomenaList = stationQuery.getPhenomena
    retval
  }

  private def readStationFromXML(stationxml : scala.xml.Node) : GLOSStation = {
    val name = (stationxml \ "name").text.trim
    val id = (stationxml \ "tag").text.trim
    val desc = (stationxml \ "description").text.trim
    val lat = (stationxml \ "latitude").text.trim.toDouble
    val lon = (stationxml \ "longitude").text.trim.toDouble
    val platform = (stationxml \ "platformtype").text.trim
    new GLOSStation(name, id, desc, platform, lat, lon)
  }
  
  private def readInMetadata() : List[scala.xml.Elem] = {
    // read in file(s), not sure how they will be reached, maybe ftp?
    // for now read in test file
    val file = scala.io.Source.fromFile(glos_metadata_file)
    val fileString = file.mkString
    file.close
//    logger.info("reading in file:\n" + fileString)
    // read in the file into xml
    val xml = scala.xml.XML.loadString(fileString)
    List(xml)
  }
  
//  private def getSourceStations() : List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
//    // read in csv file and parse each line into a station to read in
//    val file = scala.io.Source.fromFile(temp_file)
//    val stationCollection = for {
//      line <- file.getLines
//      if(!line.contains("row_id"))
//      val station = readStationFromLine(line)
//      val dbStation = new DatabaseStation(station.stationName, station.stationId, station.stationId, station.stationDesc, "BUOY", SourceId.GLOS, station.lat, station.lon)
//      val sourceObservedProperties = getObservedProperties
//      val databaseObservedProperties = stationUpdater.updateObservedProperties(source, sourceObservedProperties)
//      val sensors = stationUpdater.getSourceSensors(dbStation, databaseObservedProperties)
//      if (sensors.nonEmpty)
//    } yield {
//      (dbStation, sensors)
//    }
//    if (stationCollection.nonEmpty) {
//      stationCollection.toList
//    }
//    else {
//      logger.info("Empty station collection!")
//      Nil
//    }
//  }
//  
//  private def getObservedProperties() : List[ObservedProperty] = {
//    List(new ObservedProperty("wdir1",SourceId.GLOS,"deg",98),
//        new ObservedProperty("wspd1",SourceId.GLOS,"m/s", 99),
//        new ObservedProperty("gust1",SourceId.GLOS,"m/s", 100),
//        new ObservedProperty("atmp1",SourceId.GLOS,"C", 101),
//        new ObservedProperty("wtmp1",SourceId.GLOS,"C", 102),
//        new ObservedProperty("rh1",SourceId.GLOS,"%", 103),
//        new ObservedProperty("dewpt1",SourceId.GLOS,"C", 104),
//        new ObservedProperty("barol",SourceId.GLOS,"atm", 105),
//        new ObservedProperty("wvhgt",SourceId.GLOS,"m", 106),
//        new ObservedProperty("dompd",SourceId.GLOS,"s", 107),
//        new ObservedProperty("mwdir",SourceId.GLOS,"deg", 108),
//        new ObservedProperty("dp001",SourceId.GLOS,"m", 109),
//        new ObservedProperty("tp001",SourceId.GLOS,"C", 110),
//        new ObservedProperty("dp002",SourceId.GLOS,"m", 109),
//        new ObservedProperty("tp002",SourceId.GLOS,"C", 110),
//        new ObservedProperty("dp003",SourceId.GLOS,"m", 109),
//        new ObservedProperty("tp003",SourceId.GLOS,"C", 110))
//  }
//  
//  private def readStationFromLine(line: String) : GlosStation = {
//    val brokenLine = line.split("\",\"").map(_.replace("\"", ""))
//    logger.info("line:\n" + line)
//    var stationId = brokenLine(6)
//    if (stationId == """\N""") {
//      val stationBreak = brokenLine(16).split("=")
//      logger.info("Breaking url:")
//      for(break <- stationBreak) {
//        logger.info(break)
//      }
//      if (stationBreak.size > 1)
//        stationId = stationBreak(1)
//      else {
//        stationId = brokenLine(28)
//        if (stationId == """\N""")
//          stationId = brokenLine(14)
//      }
//    }
//    var lat = 0d
//    var lon = 0d
//    if (brokenLine(7) == """\N""")
//      lon = -9999d
//    else
//      lon = brokenLine(7).toDouble
//    if (brokenLine(8) == """\N""")
//      lat = -9999d
//    else
//      lat = brokenLine(8).toDouble
//    logger.info("Populating station: " + brokenLine(14) + ", " + stationId + ", " + brokenLine(15) + ", " + lat + ", " + lon)
//    new GlosStation(brokenLine(14), stationId, brokenLine(15), lat, lon)
//  }
}
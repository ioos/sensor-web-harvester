/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.data.LocalPhenomenon
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

    val databaseStations = stationQuery.getAllStations(source)

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
      val observedProperties = getObservedProperties((sensor \ "Phenomena"))
      val dbObservedProperties = stationUpdater.updateObservedProperties(source, observedProperties)
    } yield {
      val phenomenaIGuess = observedProperties.map(p => p.phenomenon.toList)
        (dbsensor, phenomenaIGuess.flatten)
    }
    val phenomSensors = for {
      phenomSensor <- (stationxml \ "Sensors" \\ "Phenomenon")
      val sensorid = (phenomSensor \ "foreigntag").text.trim
      val sensordesc = (phenomSensor \ "name").text.trim
      val dbsensor = new DatabaseSensor(sensorid, sensordesc, station.id)
      val observedProperties = getObservedProperties(phenomSensor)
      val dbObservedProperties = stationUpdater.updateObservedProperties(source, observedProperties)
    } yield {
        val phenomenaSupposedly = observedProperties.map(p => p.phenomenon.toList)
        (dbsensor, phenomenaSupposedly.flatten)
    }
    sensors.toList ::: phenomSensors.toList
  }
  
  private def getObservedProperties(parentNode: scala.xml.NodeSeq) : List[ObservedProperty] = {
    val phenomena = for {
      phenomena <- (parentNode \\ "Phenomenon")
    } yield {
      getObservedProperty(findPhenomenon((phenomena \ "tag").text.trim, (phenomena \ "units").text.trim),
                          (phenomena \ "tag").text.trim,
                          (phenomena \ "description").text.trim,
                          (phenomena \ "name").text.trim)
    }
    phenomena.toList.filter(p => p.isDefined).map(p => p.get)
  }
  
  private def findPhenomenon(tag: String, units: String) : Phenomenon = {
    val ltag = tag.toLowerCase
    // check the tag to list of known phenomena
    if (ltag contains "wdir") {
      return Phenomena.instance.WIND_FROM_DIRECTION
    } else if (ltag contains "wspd") {
      return Phenomena.instance.WIND_SPEED
    } else if (ltag contains "gust") {
      return Phenomena.instance.WIND_SPEED_OF_GUST
    } else if (ltag contains "atmp") {
      return Phenomena.instance.AIR_TEMPERATURE
    } else if (ltag contains "wtmp") {
      return Phenomena.instance.SEA_WATER_TEMPERATURE
    } else if (ltag contains "rh") {
      return Phenomena.instance.RELATIVE_HUMIDITY
    } else if (ltag contains "dewpt") {
      return Phenomena.instance.DEW_POINT_TEMPERATURE
    } else if (ltag contains "baro") {
      return Phenomena.instance.AIR_PRESSURE
    } else if (ltag contains "wvhgt") {
      return Phenomena.instance.SEA_SURFACE_SWELL_WAVE_SIGNIFICANT_HEIGHT
    } else if (ltag contains "dompd") {
      return Phenomena.instance.SEA_SURFACE_SWELL_WAVE_PERIOD
    } else if (ltag contains "mwdir") {
      return Phenomena.instance.SEA_SURFACE_SWELL_WAVE_TO_DIRECTION
    } else if (ltag contains "tp") {
      return Phenomena.instance.SEA_WATER_TEMPERATURE
    }
    // create a phenomena
    Phenomena.instance.createHomelessParameter(ltag, units)
  }
    
  private def getObservedProperty(phenomenon: Phenomenon, foreignTag: String, desc: String, name: String) : Option[ObservedProperty] = {
    try {
      var localPhenom: LocalPhenomenon = new LocalPhenomenon(new DatabasePhenomenon(phenomenon.getId))
      var units: String = if (phenomenon.getUnit == null || phenomenon.getUnit.getSymbol == null) "none" else phenomenon.getUnit.getSymbol
      if (localPhenom.databasePhenomenon.id < 0) {
        localPhenom = new LocalPhenomenon(insertPhenomenon(localPhenom.databasePhenomenon, units, desc, name))
      }
      return new Some[ObservedProperty](stationUpdater.createObservedProperty(foreignTag, source, localPhenom.getUnit.getSymbol, localPhenom.databasePhenomenon.id))
    } catch {
      case ex: Exception => {}
    }
    None
  }

  private def insertPhenomenon(dbPhenom: DatabasePhenomenon, units: String, description: String, name: String) : DatabasePhenomenon = {
    dbPhenom.units = units
    dbPhenom.description = description
    dbPhenom.name = name
    stationQuery.createPhenomenon(dbPhenom)
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
}
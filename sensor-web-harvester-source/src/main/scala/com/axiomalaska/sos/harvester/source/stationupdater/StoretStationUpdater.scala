/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.harvester.source.stationupdater

import java.net.URLEncoder

import scala.Array.canBuildFrom
import scala.Option.option2Iterable

import org.apache.log4j.Logger

import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.harvester.BoundingBox;
import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.data.DatabasePhenomenon;
import com.axiomalaska.sos.harvester.data.DatabaseSensor;
import com.axiomalaska.sos.harvester.data.DatabaseStation;
import com.axiomalaska.sos.harvester.data.ObservedProperty;
import com.axiomalaska.sos.harvester.data.SourceId;
import com.axiomalaska.sos.tools.{HttpSender, HttpPart}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.parallel._

import org.apache.log4j.Logger
import java.sql.Timestamp
import org.joda.time.format.DateTimeFormat

case class StoretStation (stationId: String, stationName: String, lat: Double, lon: Double, orgId: String)

// Set the environment var STORET_FULL_UPDATE to any value to update every found station.
// Otherwise all existing stations in the database are ignored.

class StoretStationUpdater (private val stationQuery: StationQuery,
                            private val boundingBox: BoundingBox) extends StationUpdater {

  private val source = stationQuery.getSource(SourceId.STORET)
  private val stationUpdater = new StationUpdateTool(stationQuery)
  private val LOGGER = Logger.getLogger(getClass())

  private val resultURLBase = "http://www.waterqualitydata.us/Result/search"
  private val stationURLBase = "http://www.waterqualitydata.us/Station/search"
  private val urlCommonOpts = List(new HttpPart("countrycode", "US"), new HttpPart("command.avoid", "NWIS"), new HttpPart("mimeType", "csv"))

  private val dateParser = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  val name = "STORET"

  def update() {
    val databaseStations = stationQuery.getAllStations(source)
    val stationTags = databaseStations.view.map {_.foreign_tag}.toSet

    val stations = getSourceStations(boundingBox, stationTags)

    // incrementally update in groups of 1000
    for ((subStations, idx) <- stations.grouped(1000).zipWithIndex) {
      val sourceStationSensors = getObservedPropsForStations(subStations, idx * 1000, stations.length)
      LOGGER.info("Saving " + sourceStationSensors.length + " to database")
      stationUpdater.updateStations(sourceStationSensors, databaseStations)
    }
  }

  private def sendGetMessage(baseUrl: String, parts: List[HttpPart]) : String = {
    // have to convert from scala immutable list to java mutable list
    // http://stackoverflow.com/questions/2429944/how-to-convert-a-scala-list-to-a-java-util-list
    val convParts: java.util.List[HttpPart] = ListBuffer(parts: _*)
    HttpSender.sendGetMessage(baseUrl, convParts)
  }

  private def getSourceStations(bbox: BoundingBox, stationTags: Set[String]) :
  List[DatabaseStation] = {
    // create the station request url by adding the bounding box
    val bboxStr = bbox.southWestCorner.getX + "," + bbox.southWestCorner.getY + "," + bbox.northEastCorner.getX + "," +
      bbox.northEastCorner.getY

    // try downloading the file ... this failed, now just load it into memory
    val response = sendGetMessage(stationURLBase, urlCommonOpts ::: List(new HttpPart("bBox", bboxStr)))
    if (response == null) {
      LOGGER error "response to station request was null"
      return Nil
    }

    val splitResponse = response.toString split '\n'
    val meh = splitResponse.filter(s => !s.contains("OrganizationIdentifier")).toList
    val stationResponse = filterCSV(meh)

    // For all non-null stations in the CSV response:
    //   Turn them into DatabaseStation instances and filter out ones that already exist in the database

    val rawStations = stationResponse.filter(p => p.nonEmpty).map(createSourceStation(_))

    scala.util.Properties.envOrNone("STORET_FULL_UPDATE") match {
      case Some(fullUpdate) => return rawStations
      case None => return rawStations.filter {t:DatabaseStation => !stationTags.contains(t.foreign_tag)}
    }
    //val stations = rawStations.filter {t:DatabaseStation => !stationTags.contains(t.foreign_tag)}
    //return stations
  }

  private def getObservedPropsForStations(stations: List[DatabaseStation], offset:Int, totallength:Int) :
  List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {

    //   In parallel, iterate these DatabaseStations:
    //     Get all phenomenas from web service
    //   For all these results:
    //     Parse phenomena and update database
    //     Get list of sensors
    // Return a list of (station, sensors) tuples

    try {

      val parStations = stations.zipWithIndex.par
      parStations.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))

      val computed = parStations.map { t:(DatabaseStation,Int) => {
        val sourceData = getStationData(t._1)
        val sourcePhenomena = getPhenomenaNameUnitDepths(sourceData)
        val timeExtents = getTimeExtents(sourceData)

        LOGGER.info("[" + (offset + t._2 + 1) + " of " + totallength + "] station: " + t._1.name)
        (t._1, sourcePhenomena, timeExtents)
      }}.seq  // convert to sequential (non-parallel)

      val retval = for {
        (station, sourcePhenomena, timeExtents) <- computed
        newStation = updateStationTimeExtents(station, timeExtents)
        phenomena = getObservedProperties(sourcePhenomena)
        dbObservedProps = stationUpdater.updateObservedProperties(source, phenomena)
        sensors = stationUpdater.getSourceSensors(station, dbObservedProps)
        if sensors.nonEmpty
      } yield {
        (newStation, sensors)
      }

      // filter out duplicate stations
      return retval.groupBy(_._1).map(_._2.head).toList

    } catch {
      case ex: Exception => LOGGER error ex.toString; ex.printStackTrace()
    }

    Nil
  }

  private def updateStationTimeExtents(station: DatabaseStation, timeExtents: (Option[Timestamp], Option[Timestamp])) : DatabaseStation = {
    val newStation = new DatabaseStation(station.name, station.tag, station.foreign_tag, station.description, station.platformType,
      station.source_id, station.latitude, station.longitude, station.active, timeExtents._1.getOrElse(null), timeExtents._2.getOrElse(null))

    return newStation
  }

  private def createSourceStation(line: String) : DatabaseStation = {
    // make sure that each string input is less than 255!!
    val name = reduceLargeStrings(getStationName(line))
    val foreignTag = reduceLargeStrings(getStationTag(line))
    // internal tag is the name, formatted
    val tag = source.tag + ":" + nameToTag(name)
    val description = reduceLargeStrings(getStationDescription(line))
    val platformType = reduceLargeStrings(getStationType(line))
    val sourceId = source.id
    val lat = getStationLatitude(line)
    val lon = getStationLongitude(line)
    val active = true

    // to get timeBegin/end we must query for the station's data - we already do this in getObservedPropsForStation,
    // so we'll update this DatabaseStation at that point.
    val timeBegin = null
    val timeEnd = null

    return new DatabaseStation(name,tag,foreignTag,description,platformType,sourceId,lat,lon,active, timeBegin, timeEnd)
  }


  private def getStationData(station: DatabaseStation) : List[String] = {
    // get the results from wqp
    val organization = (station.foreign_tag split "-")
    // skip this organization since it has several hundred stations, all empty
    if (organization.head.equalsIgnoreCase("1117MBR"))
      return List()

    try {
      val response = sendGetMessage(resultURLBase, urlCommonOpts ::: List(new HttpPart("siteid", station.foreign_tag)))
      if (response != null) {
        val splitResponse = response.mkString.split('\n').drop(1).toList
        return filterCSV(splitResponse)
      }
    } catch {
      case ex: Exception => {
        LOGGER error ex.toString
        ex.printStackTrace()
      }
    }

    return List()
  }

  private def getTimeExtents(sourceData: List[String]) : (Option[Timestamp], Option[Timestamp]) = {
    val timeList = for {
      timeRow <- sourceData
      cols = timeRow split ","
    } yield {
      val beginTime = if (cols(6) != "") Some(dateParser.parseDateTime(cols(6) + " " + cols(7))) else None
      val endTime = if (cols(9) != "") Some(dateParser.parseDateTime(cols(9) + " " + cols(10))) else None
      (beginTime.map(_.getMillis), endTime.map(_.getMillis))
    }

    val (beginTimeList, endTimeList) = timeList unzip
    val begin = if (beginTimeList.filter(_.isDefined).length > 0) beginTimeList.min else None
    val end = if (endTimeList.filter(_.isDefined).length > 0) endTimeList.max else None

    return (begin.map(new Timestamp(_)), end.map(new Timestamp(_)))
  }

  private def getObservedProperties(phenomena: List[(String,String,List[String])]) : List[ObservedProperty] = {
    val proplistlist = for {
      (name, units, depths) <- phenomena
      if (!name.contains("text"))
      depth <- depths
    } yield {
      stationUpdater.getObservedProperty(
        matchPhenomenaToName(name, fixUnitsString(units)),
        name, depth, source)
    }

    proplistlist
  }

  private def filterCSV(csv: List[String]) : List[String] = {
    var inQuote: Boolean = false
    csv.map( l => {
      //        LOGGER.info("Line before filter:\n" + l)
      val newString = for (ch <- l) yield ch match {
        case '"' if (!inQuote) => { inQuote = true; '\1' }
        case ',' if (inQuote) => '\0'
        case '"' if (inQuote) => { inQuote = false; '\1' }
        case default => default
      }
      val ns = newString.filter(_ != '\1')
      //        LOGGER.info("line after filter:\n" + ns)
      ns
    } )
  }

  private def reduceLargeStrings(str: String) : String = {
    if (str.length > 254)
      str.substring(0, 255)
    else
      str
  }

  private def fixUnitsString(units: String) : String = {
    // fix any unit string issues here (ex: cannot contain '#' and '/' should be replaced with a '.' and then add a '-1')
    var retval = units.replaceAll("#", "parts")
    retval
  }

  private def nameToTag(name: String) : String = {
    name.trim.toLowerCase.replaceAll("""[\s-]+""", "_").replaceAll("""[\W]+""", "")
  }

  private def matchPhenomenaToName(name: String, units: String) : Phenomenon = {
    val lname = name.toLowerCase
    if (lname.equals("ammonium") || lname.equals("ammonium as n")) {
      Phenomena.instance.AMMONIUM
    } else if (lname.equals("chlorophyll") || lname.equals("chlorophyll a free of pheophytin")) {
      Phenomena.instance.CHLOROPHYLL
    } else if (lname equals "chlorophyll_flourescence") {
      Phenomena.instance.CHLOROPHYLL_FLOURESCENCE
    } else if (lname.equals("nitrite+nitrate") || lname.equals("inorganic nitrogen (nitrate and nitrite) as n")) {
      Phenomena.instance.NITRITE_PLUS_NITRATE
    } else if (lname equals "nitrite") {
      Phenomena.instance.NITRITE
    } else if (lname equals "nitrate") {
      Phenomena.instance.NITRATE
    } else if (lname.equals("temperature water")) {
      Phenomena.instance.SEA_WATER_TEMPERATURE
    } else if (lname equals "speed water") {     // not sure if this is actually a variable name in storet
      Phenomena.instance.SEA_WATER_SPEED
    } else if (lname.equals("phosphorus as p")) {
      Phenomena.instance.PHOSPHORUS
    } else if (lname.equals("wind direction") || lname.equals("wind direction (direction from expressed 0-360 deg)")) {  // not sure if this is actually a variable name in storet
      Phenomena.instance.WIND_FROM_DIRECTION
    } else if (lname equals "wind gust") {       // not sure if this is actually a variable name in storet
      Phenomena.instance.WIND_SPEED_OF_GUST
    } else if (lname.equals("temperature air")) {
      Phenomena.instance.AIR_TEMPERATURE
    } else if (lname equals "dew") {
      Phenomena.instance.DEW_POINT_TEMPERATURE
    } else if (lname equals "ph") {
      Phenomena.instance.SEA_WATER_PH_REPORTED_ON_TOTAL_SCALE
    } else if (lname.equals("alkalinity total (total hydroxide+carbonate+bicarbonate)") || lname.equals("alkalinity total as caco3")) {
      Phenomena.instance.ALKALINITY
    } else if (lname.equals("wave height")) {
      Phenomena.instance.SEA_SURFACE_WIND_WAVE_SIGNIFICANT_HEIGHT
    } else if (lname.equals("water level reference point elevation") || lname.equals("water level in relation to reference point")) {
      Phenomena.instance.WATER_SURFACE_HEIGHT_ABOVE_REFERENCE_DATUM
    } else if (lname.equals("specific conductance")) {
      Phenomena.instance.SEA_WATER_ELECTRICAL_CONDUCTIVITY
    } else if (units contains "#/100ml") {
      Phenomena.instance.createHomelessParameter(nameToTag(lname),"#.100mL-1")
    } else if (units.toLowerCase contains "ug/l") {
      Phenomena.instance.createHomelessParameter(nameToTag(lname),"10e-1mg.L-1")
    } else if (units.toLowerCase contains "cfu") {
      Phenomena.instance.createHomelessParameter(nameToTag(lname),"cfu.100mL-1")
    } else {
      // create a homeless parameter
      LOGGER.debug("Unknown phenomenon: " + lname + " " + units)
      Phenomena.instance.createHomelessParameter(nameToTag(lname), units)
    }
  }

  private def getPhenomenaNameUnitDepths(station: List[String]) : List[(String,String,List[String])] = {
    // go through the results and get the names of all lines w/o 'Non-detect'
    val splitLines = station filter { !_.contains("Non-detect") } map { _.split(",") }
    val fixed = splitLines.map(_.map(_.replaceAll("\0", "")))
    // 'charactername' is at index 31, 'units' string is index 34
    val phenomMap = (fixed map { _.zipWithIndex }) map { s => s.foldRight("","","")((nindex,retval) => nindex._2 match {
      case 31 => (nindex._1,retval._2,retval._3)
      case 34 => (retval._1,nindex._1,retval._3)
      case 12 => (retval._1,retval._2,nindex._1)
      case _ => retval
    } ) }
    phenomMap.groupBy( _._1 ).map( sl => {
      val depths = sl._2.map(m => m._3)
      val unitcode = sl._2.head._2
      (sl._1,unitcode,depths)
    } ).toList
  }

  private def getStationName(station: String) : String = {
    val splitLines = (station split ",")
    // okay so below allows us to search by index looking for the earliest instance of index 3
    val retval = (splitLines zipWithIndex).foldRight("",0)((a,b) => { b._2 match { case 3 => (b._1,b._2); case _ => (a._1,a._2) } } )._1
    retval.replaceAll("\0", ",")
  }

  private def getStationTag(station: String) : String = {
    val splitLines = (station split ",")
    // okay so below allows us to search by index looking for the earliest instance of index 2
    val retval = (splitLines zipWithIndex).foldRight("",0)((a,b) => { b._2 match { case 2 => (b._1,b._2); case _ => (a._1,a._2) } } )._1
    retval.replaceAll("\0", ",")
  }

  private def getStationDescription(station: String) : String = {
    val splitLines = (station split ",")
    // okay so below allows us to search by index looking for the earliest instance of index 5
    val retval = (splitLines zipWithIndex).foldRight("",0)((a,b) => { b._2 match { case 5 => (b._1,b._2); case _ => (a._1,a._2) } } )._1
    retval.replaceAll("\0",",")
  }

  private def getStationType(station: String) : String = {
    val splitLines = (station split ",")
    // okay so below allows us to search by index looking for the earliest instance of index 4
    val retval = (splitLines zipWithIndex).foldRight("",0)((a,b) => { b._2 match { case 4 => (b._1,b._2); case _ => (a._1,a._2) } } )._1
    retval.replaceAll("\0", ",")
  }

  private def getStationLatitude(station: String) : Double = {
    val splitLines = (station split ",")
    // okay so below allows us to search by index looking for the earliest instance of index 11
    val lat = (splitLines zipWithIndex).foldRight("",0)((a,b) => { b._2 match { case 11 => (b._1,b._2); case _ => (a._1,a._2) } } )._1
    try {
      return java.lang.Double.parseDouble(lat)
    } catch {
      case ex: Exception => {
        LOGGER error ex.toString
      }
    }
    Double.NaN
  }

  private def getStationLongitude(station: String) : Double = {
    val splitLines = (station split ",")
    // okay so below allows us to search by index looking for the earliest instance of index 12
    val lon = (splitLines zipWithIndex).foldRight("",0)((a,b) => { b._2 match { case 12 => (b._1,b._2); case _ => (a._1,a._2) } } )._1
    try {
      return java.lang.Double.parseDouble(lon)
    } catch {
      case ex: Exception => LOGGER error ex.toString
    }
    Double.NaN
  }
}

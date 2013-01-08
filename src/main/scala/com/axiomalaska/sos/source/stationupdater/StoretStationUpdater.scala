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
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.phenomena.Phenomena

import org.apache.log4j.Logger

case class StoretStation (stationId: String, stationName: String, lat: Double, lon: Double, orgId: String)

class StoretStationUpdater (private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  private val source = stationQuery.getSource(SourceId.STORET)
  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val httpSender = new HttpSender()
  
  private val resultURL = "http://www.waterqualitydata.us/Result/search?countrycode=US&command.avoid=NWIS&mimeType=csv"
  private val stationURL = "http://www.waterqualitydata.us/Station/search?countrycode=US&command.avoid=NWIS&mimeType=csv"
  
  private var phenomenaList = stationQuery.getPhenomena
  
  var stationResponse: List[String] = null
  var resultResponse: (String,List[String]) = ("",Nil)
  
  val name = "STORET"

  def update() {
    val sourceStationSensors = getSourceStations(boundingBox)

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }
  
  
  private def getSourceStations(bbox: BoundingBox) : List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    try {
      // create the station request url by adding the bounding box
      val requestURL = stationURL + "&bBox=" + bbox.southWestCorner.getLongitude + "," + bbox.southWestCorner.getLatitude + "," + bbox.northEastCorner.getLongitude + "," + bbox.northEastCorner.getLatitude
      // try downloading the file ... this failed, now just load it into memory
      val response = httpSender.sendGetMessage(requestURL)
      if (response != null) {
        val splitResponse = response.toString split '\n'
        val meh = splitResponse.filter(s => !s.contains("OrganizationIdentifier")).toList
        stationResponse = filterCSV(meh)
        logger.info("Collected " + stationResponse.size + " lines of metadata")
        // go through the list, compiling all of the stations
        val retval = for {
          (stationLine, index) <- stationResponse.zipWithIndex
          station <- createSourceStation(stationLine)
          if (!stationQuery.getStation(station.foreign_tag).isDefined)
          val sourceOP = getSourceObservedProperties(station)
          val databaseObservedProperties =
            stationUpdater.updateObservedProperties(source, sourceOP)
          val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
          if(sensors.nonEmpty)
        } yield {
          logger.debug("[" + index + " of " + (stationResponse.length - 1) + "] station: " + station.name)
          (station, sensors)
        }
        // filter out duplicate stations
        return retval.groupBy(_._1).map(_._2.head).toList
      } else {
        logger error "response to " + requestURL + " was null"
      }
    } catch {
      case ex: Exception => logger error ex.toString; ex.printStackTrace()
    }
    
    Nil
  }
  
  private def createSourceStation(line: String) : Option[DatabaseStation] = {
    if (line != null) {
      // make sure that each string input is less than 255!!
      val name = reduceLargeStrings(getStationName(line))
      val foreignTag = reduceLargeStrings(getStationTag(line))
      // internal tag is the name, formatted
      val tag = nameToTag(name)
      val description = reduceLargeStrings(getStationDescription(line))
      val platformType = reduceLargeStrings(getStationType(line))
      val sourceId = source.id
      val lat = getStationLatitude(line)
      val lon = getStationLongitude(line)
      val active = true
      return Some(new DatabaseStation(name,tag,foreignTag,description,platformType,sourceId,lat,lon,active))
    }
    None
  }
  
  private def getSourceObservedProperties(station: DatabaseStation) : List[ObservedProperty] = {
    // get the results from wqp
    val organization = (station.foreign_tag split "-")
    // skip this organization since it has several hundred stations, all empty
    if (organization.head.equalsIgnoreCase("1117MBR"))
      return Nil

    if (!resultResponse._1.equalsIgnoreCase(station.foreign_tag)) {
      try {
        val request = resultURL + "&siteid=" + station.foreign_tag + "&organization=" + organization.head
        logger debug "Sending request: " + request
        val response = httpSender.sendGetMessage(request)
        if (response != null) {
          val splitResponse = response.mkString.split('\n')
          val removeFirstRow = splitResponse.filter(!_.contains("OrganizationIdentifier")).toList
          resultResponse = (station.foreign_tag,filterCSV(removeFirstRow))
        }
      } catch {
        case ex: Exception => {
            logger error ex.toString
            ex.printStackTrace()
            resultResponse = ("",Nil)
        }
      }
    }
    
    if (resultResponse._2 == Nil)
      return Nil
    
    val phenomena = getPhenomenaNameUnitDepths(resultResponse._2)
    val proplistlist = for {
      phenomenon <- phenomena
      if (!phenomenon._1.contains("text"))
    } yield {
      for {
        depth <- phenomenon._3
        observedProp <- getObservedProperty(matchPhenomenaToName(phenomenon._1, fixUnitsString(phenomenon._2)), phenomenon._1, depth)
      } yield {
        observedProp
      }
    }
  
    proplistlist.flatten
  }
  
  private def filterCSV(csv: List[String]) : List[String] = {
      var inQuote: Boolean = false
      csv.map( l => {
//        logger.info("Line before filter:\n" + l)
        val newString = for (ch <- l) yield ch match {
          case '"' if (!inQuote) => { inQuote = true; '\1' }
          case ',' if (inQuote) => '\0'
          case '"' if (inQuote) => { inQuote = false; '\1' }
          case default => default
        }
        val ns = newString.filter(_ != '\1')
//        logger.info("line after filter:\n" + ns)
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
      Phenomena.instance.createPhenomenonWithPPmL(nameToTag(lname))
    } else if (units.toLowerCase contains "ug/l") {
      Phenomena.instance.createPhenomenonWithugL(nameToTag(lname))
    } else if (units.toLowerCase contains "cfu") {
      Phenomena.instance.createPhenonmenonWithCFU(nameToTag(lname))
    } else {
      // create a homeless parameter
      Phenomena.instance.createHomelessParameter(nameToTag(lname), units)
    }
  }
  
  private def getObservedProperty(phenomenon: Phenomenon, foreignTag: String, depth: String) : Option[ObservedProperty] = {
    try {
      var localPhenom: LocalPhenomenon = new LocalPhenomenon(new DatabasePhenomenon(phenomenon.getId))
      var units: String = if (phenomenon.getUnit == null || phenomenon.getUnit.getSymbol == null) "none" else phenomenon.getUnit.getSymbol
      var ddepth: Double = 0
      try {
        ddepth = java.lang.Double.parseDouble(depth)
      } catch {
        case ex: Exception => logger debug ex.toString
      }
      
      if (localPhenom.databasePhenomenon.id < 0) {
        localPhenom = new LocalPhenomenon(insertPhenomenon(localPhenom.databasePhenomenon, units, phenomenon.getId, phenomenon.getName))
      }
      return new Some[ObservedProperty](stationUpdater.createObservedProperty(foreignTag, source, localPhenom.getUnit.getSymbol, localPhenom.databasePhenomenon.id,ddepth))
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
            logger error ex.toString
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
        case ex: Exception => logger error ex.toString
      }
      Double.NaN
  }
}
  
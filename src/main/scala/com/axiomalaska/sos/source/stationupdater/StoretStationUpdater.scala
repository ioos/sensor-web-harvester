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
  // value that determines the maximum number of stations that will be made in a single getStation request
  // the harvester will cut up the bbox until each segment has a number of stations at or under this value
  private val stationBlockLimit = 300
  
  private val resultURL = "http://www.waterqualitydata.us/Result/search?countrycode=US&command.avoid=NWIS&mimeType=csv"
  private val stationURL = "http://www.waterqualitydata.us/Station/search?countrycode=US&command.avoid=NWIS&mimeType=csv"
  
  private var phenomenaList = stationQuery.getPhenomena
  
  var stationResponse: List[String] = null
  var resultResponse: List[String] = null
  
  val name = "STORET"

  def update() {
    val sourceStationSensors = getSourceStations(boundingBox)

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }
  
  
  private def getSourceStations(bbox: BoundingBox) : List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    // create the station request url by adding the bounding box
    val requestURL = stationURL + "&bBox=" + bbox.southWestCorner.getLongitude + "," + bbox.southWestCorner.getLatitude + "," + bbox.northEastCorner.getLongitude + "," + bbox.northEastCorner.getLatitude
    // try downloading the file ... this failed, now just load it into memory
    val response = httpSender.sendGetMessage(requestURL)
    if (response != null) {
      val splitResponse = response.toString split '\n'
      val removeFirstRow = splitResponse.toList.filter(s => !s.contains("OrganizationIdentifier"))
      stationResponse = filterCSV(removeFirstRow)
      // go through the list, compiling all of the stations
      for {
        (stationLine, index) <- stationResponse.zipWithIndex
        station <- createSourceStation(stationLine)
        val sourceOP = getSourceObservedProperties(station)
        val databaseObservedProperties =
          stationUpdater.updateObservedProperties(source, sourceOP)
        val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
        if(sensors.nonEmpty)
      } yield {
        logger.debug("[" + index + " of " + (stationResponse.length - 1) + "] station: " + station.name)
        (station, sensors)
      }
    } else {
      logger error "response to " + requestURL + " was null"
    }
    
    Nil
  }
  
  private def createSourceStation(line: String) : Option[DatabaseStation] = {
    if (line != null) {
      val name = getStationName(line)
      val foreignTag = getStationTag(line)
      // internal tag is the name, formatted
      val tag = nameToTag(name)
      val description = getStationDescription(line)
      val platformType = getStationType(line)
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
    val request = resultURL + "&siteid=" + station.foreign_tag + "&organization=" + organization.head
    logger info "Sending request: " + request
    val response = httpSender.sendGetMessage(request)
    if (response != null) {
      val splitResponse = response.toString split '\n'
      val removeFirstRow = splitResponse.toList.filter(s => !s.contains("OrganizationIdentifier"))
      resultResponse = filterCSV(removeFirstRow)
      // get a list of characteristic names (phenomena)
      val phenomena = getPhenomenaNameAndUnit(resultResponse)
      // return a list of observedProperties
      for {
        phenomenon <- phenomena
        if (!phenomenon._1.contains("text"))  // don't include phenomenon that are only textual descriptions (not actual measurements)
        observedProp <- getObservedProperty(matchPhenomenaToName(phenomenon._1, fixUnitsString(phenomenon._2)), phenomenon._1)
      } yield {
        observedProp
      }
    } else {
      logger error "Unable to process previous request"
      Nil
    }
  }
  
  private def filterCSV(csv: List[String]) : List[String] = {
      var inQuote: Boolean = false
      csv map { l => {
        val newString = for (ch <- l) yield ch match {
          case '"' if (!inQuote) => { inQuote = true; '\0' }
          case ',' if (inQuote) => '\0'
          case '"' if (inQuote) => { inQuote = false; '\0' }
          case default => default
        }
        newString filter ( _ != '\0' )
      } }
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
    logger.info("Looking for phenom " + name + " | " + units)
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
  
  private def getObservedProperty(phenomenon: Phenomenon, foreignTag: String) : Option[ObservedProperty] = {
    try {
      var localPhenom: LocalPhenomenon = new LocalPhenomenon(new DatabasePhenomenon(phenomenon.getId))
      var units: String = if (phenomenon.getUnit == null || phenomenon.getUnit.getSymbol == null) "none" else phenomenon.getUnit.getSymbol
      if (localPhenom.databasePhenomenon.id < 0) {
        localPhenom = new LocalPhenomenon(insertPhenomenon(localPhenom.databasePhenomenon, units, phenomenon.getId, phenomenon.getName))
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
  
  private def getPhenomenaNameAndUnit(station: List[String]) : List[(String,String)] = {
    // go through the results and get the names of all lines w/o 'Non-detect'
    val splitLines = station filter { !_.contains("Non-detect") } map { _.split(",") }
    // 'charactername' is at index 31, 'units' string is index 34
    val phenomMap = (splitLines map { _.zipWithIndex }) map { s => s.foldRight("",0,"")((n,o) => n._2 match { case 31 => (n._1,o._2,o._3); case 34 => (o._1,o._2,n._1); case _ => (o._1,o._2,o._3) } ) }
    phenomMap.groupBy( _._1 ).map( sl => (sl._1,sl._2.head._3) ).toList
  }
  
  private def getStationName(station: String) : String = {
      val splitLines = (station split ",")
      // okay so below allows us to search by index looking for the earliest instance of index 3
      (splitLines zipWithIndex).foldRight("",0)((a,b) => { b._2 match { case 3 => (b._1,b._2); case _ => (a._1,a._2) } } )._1
  }
  
  private def getStationTag(station: String) : String = {
      val splitLines = (station split ",")
      // okay so below allows us to search by index looking for the earliest instance of index 2
      (splitLines zipWithIndex).foldRight("",0)((a,b) => { b._2 match { case 2 => (b._1,b._2); case _ => (a._1,a._2) } } )._1
  }
  
  private def getStationDescription(station: String) : String = {
      val splitLines = (station split ",")
      // okay so below allows us to search by index looking for the earliest instance of index 5
      (splitLines zipWithIndex).foldRight("",0)((a,b) => { b._2 match { case 5 => (b._1,b._2); case _ => (a._1,a._2) } } )._1
  }
  
  private def getStationType(station: String) : String = {
      val splitLines = (station split ",")
      // okay so below allows us to search by index looking for the earliest instance of index 4
      (splitLines zipWithIndex).foldRight("",0)((a,b) => { b._2 match { case 4 => (b._1,b._2); case _ => (a._1,a._2) } } )._1
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
  
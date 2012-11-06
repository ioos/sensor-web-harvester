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
  private val stationBlockLimit = 250
  
  private val resultURL = "http://ofmpub.epa.gov/STORETwebservices/StoretResultService/"
  private val stationURL = "http://ofmpub.epa.gov/STORETwebservices/StationService/"
  
  private var phenomenaList = stationQuery.getPhenomena

  def update() {
    val bboxList = divideBBoxIntoChunks(boundingBox, List())
    
    for (bbox <- bboxList) {
      logger.info("Proccessing bbox: " + bbox.toString)
      val sourceStationSensors = getSourceStations(bbox)

      val databaseStations = stationQuery.getAllStations(source)

      stationUpdater.updateStations(sourceStationSensors, databaseStations)
    }
  }
  
  val name = "STORET"
  
  private def getSourceStations(bbox: BoundingBox) : List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
      // get lat, lon, name, id, any description, and platform (if that is possible); create database station for each result
      val xml = getStationsForMap(bbox)
      val stations = xml match {
        case Some(xml) => {
            val slist = for (val row <- xml \\ "Organization") yield {
             val sublist = for (val srow <- row \\ "MonitoringLocation") yield {
                val stationId = (srow \\ "MonitoringLocationIdentifier").text
                val stationName = (srow \\ "MonitoringLocationName").text
                val lat = (srow \\ "LatitudeMeasure").text
                val lon = (srow \\ "LongitudeMeasure").text
                StoretStation(stationId, stationName, lat.toDouble, lon.toDouble, (row \ "OrganizationDescription" \ "OrganizationIdentifier").text)
              }
              sublist.toList
            }
            slist.toList.flatten
        }
        case None => {
            logger.info("got None for getStationsForMap!")
            Nil
        }
        case _ => {
            logger.info("unhandled case: " + xml)
            Nil
        }
      }
      
    val flatStationList = stations.toList
   // have list of stations to iterate through
   if (flatStationList != Nil) {
     val size = flatStationList.length
     val stationCollection = for {
       (station, index) <- flatStationList.zipWithIndex
       val sourceObservedProperties = getObservedProperties(station.stationId, station.orgId)
       val dbStation = new DatabaseStation(station.stationName, station.stationId, station.stationId, station.orgId, "Watershed", source.id, station.lat, station.lon)
       val databaseObservedProperties = stationUpdater.updateObservedProperties(source, sourceObservedProperties)
       val sensors = stationUpdater.getSourceSensors(dbStation, databaseObservedProperties)
       if (sensors.nonEmpty)
    } yield {
      logger.info("[" + (index+1).toString + " of " + size + "] station: " + station.stationId + "::" + station.orgId)
      (dbStation, sensors)
    }
    stationCollection.toList
   } else {
     Nil
   }
  }
  
  private def getObservedProperties(stationId: String, orgId: String) : List[ObservedProperty] = {
    // query the characteristics of each org-station
    val xml = <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:srs="http://storetresultservice.storet.epa.gov/">
                <soap:Body>
                  <srs:getResults>
                    <OrganizationId>{orgId}</OrganizationId>
                    <MonitoringLocationId>{stationId}</MonitoringLocationId>
                    <MonitoringLocationType/><MinimumActivityStartDate/><MaximumActivityStartDate/><MinimumLatitude/><MaximumLatitude/><MinimumLongitude/><MaximumLongitude/><CharacteristicType/><CharacteristicName/><ResultType/>
                  </srs:getResults>
                </soap:Body>
              </soap:Envelope>
    val response = httpSender.sendPostMessage(resultURL, xml.toString)
    //    use a test file
//    val file = scala.io.Source.fromFile("ex_getResultsResponse.xml");
//    val response = file.mkString
//    file.close()
    if (response != null) {
      logger.debug("processing properties for " + stationId + " - " + orgId)
      val responseFix = fixResponseString(response.toString)
      val root = loadXMLFromString(responseFix)
      root match {
        case Some(root) => {
          var charNameList:List[String] = Nil
          val results = for {
            node <- (root \\ "ResultDescription")
            if (node.nonEmpty)
            if (!((node \\ "ResultDetectionConditionText").exists(_.text.toLowerCase contains "non-detect") || (node \\ "ResultDetectionConditionText").exists(_.text.toLowerCase contains "present")))
            if ((node \\ "ResultMeasureValue").text.nonEmpty && (node \\ "ResultMeasureValue").text.trim != "")
            val name = (node \ "CharacteristicName").text
            if (!charNameList.exists(p => p == name))
          } yield {
            charNameList = name :: charNameList
            (name, node)
          }
          results.toList.flatMap(property => getObservedProperty(property._1, property._2))
        }
        case None => {
            Nil
        }
      }
      
    } else
      Nil
  }
  
  private def getObservedProperty(name: String, result: scala.xml.NodeSeq) : Option[ObservedProperty] = {
    // condition on name as some of the names are stupidly long and difficult to parse
    // iterate through phenomena list to see if this exists in there
    var tag = name.trim.toLowerCase.replaceAll("""[\s-]+""", "_").replaceAll("""[\W]+""", "")
    // add special cases here
    tag = specialCaseTags(tag)
    var units = (result \ "ResultMeasure" \ "MeasureUnitCode").text
    units = if (units.nonEmpty) units.trim else "None"
    units = specialCaseUnits(units)
    val phenom = findPhenomenon(tag, units)
    try {
      var lPhenom = new LocalPhenomenon(new DatabasePhenomenon(phenom.getId))
      if (lPhenom.databasePhenomenon.id < 0) {
        // create the phenomenon
        logger.info("Inserting phenomenon " + lPhenom.getId + " into database")
        val unitString = if (phenom.getUnit != null && phenom.getUnit.getSymbol != null && !phenom.getUnit.getSymbol.equalsIgnoreCase("")) phenom.getUnit.getSymbol else units
        lPhenom = new LocalPhenomenon(insertPhenomenon(lPhenom.databasePhenomenon, unitString, "", lPhenom.getName))
      }
      return new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, lPhenom.databasePhenomenon.units, lPhenom.databasePhenomenon.id))
    } catch {
      case ex: Exception => {
//          try {
//            var nPhenom: DatabasePhenomenon = new DatabasePhenomenon(phenom.getId)
//            if (phenom != null && phenom.getUnit != null && phenom.getUnit.getSymbol != null && !phenom.getUnit.getSymbol.equalsIgnoreCase("")) {
//              nPhenom.units = phenom.getUnit.getSymbol
//            } else {
//              nPhenom.units = units
//            }
//            nPhenom.name = phenom.getName
//            logger.info("Will create phenom - " + nPhenom.tag)
//            return new Some[ObservedProperty](
//              stationUpdater.createObservedProperty(name, source, nPhenom.units, stationQuery.createPhenomenon(nPhenom).id))
//          } catch {
//            case ex: Exception =>{
//                logger.info("Unable to create observed property: " + name)
//                logger.warn("Exception in getObservedProperty -\n" + ex.toString + ":\n" + ex.getStackTraceString)
//                exit(-1)
//                None
//            }
//          }
          logger.info("Unable to create observed property: " + name)
          logger.warn("Exception in getObservedProperty -\n" + ex.toString + ":\n" + ex.getStackTraceString)
          None
      }
    }
  }
  
  private def insertPhenomenon(dbPhenom: DatabasePhenomenon, units: String, description: String, name: String) : DatabasePhenomenon = {
    dbPhenom.units = units
    dbPhenom.description = description
    dbPhenom.name = name
    stationQuery.createPhenomenon(dbPhenom)
  }
  
  private def specialCaseTags(tag : String) : String = {
    tag match {
      case "ph" => {
          "fresh_water_acidity"
      }
      case _ => {
          tag
      }
    }
  }
  
  private def findPhenomenon(name: String, units: String) : Phenomenon = {
    val lname = name.toLowerCase
    if (lname contains "ammonium") {
      return Phenomena.instance.AMMONIUM
    } else if (lname.contains("chlorophyll") && lname.contains("fluorescence")) {
      return Phenomena.instance.CHLOROPHYLL_FLOURESCENCE
    }  else if (lname contains "nitrite") {
      if (lname.contains("+")) {
        return Phenomena.instance.NITRITE_PLUS_NITRATE
      }
      // else it is prob just nitrite
      return Phenomena.instance.NITRITE
    } else if (lname contains "nitrate") {
      return Phenomena.instance.NITRATE
    } else if (lname contains "water") {
      if (lname contains "temperature") {
        return Phenomena.instance.SEA_WATER_TEMPERATURE
      }
    } else if (lname contains "wind") {
      if (lname contains "direction") {
        return Phenomena.instance.WIND_FROM_DIRECTION
      }
    } else if (lname contains "gust") {
      return Phenomena.instance.WIND_SPEED_OF_GUST
    } else if (lname contains "dew") {
      return Phenomena.instance.DEW_POINT_TEMPERATURE
    }
    // create a homeless parameter
    logger.info("Creating a homeless parameter for " + name + " - " + units)
//    stationQuery.createPhenomenon(new DatabasePhenomenon(lname))
    val phenomena = Phenomena.instance.createHomelessParameter(lname, units)
    if (phenomena == null) {
      logger.error("Null phenomena instance created with units - " + units)
    }
    return phenomena
  }
  
  private def specialCaseUnits(units : String) : String = {
    if (units contains "deg") {
      units.replaceAll("deg", "").trim
    }
    else if (units contains "degree") {
      units.replaceAll("degree", "").trim
    }
    else {
      units
    }
  }
  
  private def getStationsForMap(bbox : BoundingBox) : Option[scala.xml.Elem] = {
    val xmlRequest = <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ss="http://stationservice.storet.epa.gov/">
                      <soap:Body>
                        <ss:getStationsForMap>
                          <MinimumLatitude>{bbox.southWestCorner.getLatitude()}</MinimumLatitude>
                          <MaximumLatitude>{bbox.northEastCorner.getLatitude()}</MaximumLatitude>
                          <MinimumLongitude>{bbox.southWestCorner.getLongitude()}</MinimumLongitude>
                          <MaximumLongitude>{bbox.northEastCorner.getLongitude()}</MaximumLongitude>
                        </ss:getStationsForMap>
                      </soap:Body>
                     </soap:Envelope>
   val response = httpSender.sendPostMessage(stationURL, xmlRequest.toString())
   // test file
//   val file = scala.io.Source.fromFile("ex_getStationsForMapResponse.xml")
//   val response = file.mkString
//   file.close()
   if (response != null) {
     val responseFix = fixResponseString(response.toString)
     loadXMLFromString(responseFix)
   }
   else
     None
  }
  
  private def divideBBoxIntoChunks(bbox : BoundingBox, list : List[BoundingBox]) : List[BoundingBox] = {
    // use the bounding box to create xml for a soap instruction
    val xml = <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ss="http://stationservice.storet.epa.gov/">
                <soap:Body>
                  <ss:getStationCount>
                    <MinimumLatitude>{bbox.southWestCorner.getLatitude()}</MinimumLatitude>
                    <MaximumLatitude>{bbox.northEastCorner.getLatitude()}</MaximumLatitude>
                    <MinimumLongitude>{bbox.southWestCorner.getLongitude()}</MinimumLongitude>
                    <MaximumLongitude>{bbox.northEastCorner.getLongitude()}</MaximumLongitude>
                  </ss:getStationCount>
                </soap:Body>
              </soap:Envelope>
    // send xml requesting the station count
    val response = httpSender.sendPostMessage(stationURL, xml.toString())
//    val response = "<count>20</count>"
    if (response != null ) {
      // can we treat response as xml?
      val responseFix = fixResponseString(response.toString)
      val responseXML = loadXMLFromString(responseFix)
      responseXML match {
        case Some(responseXML) => {
            val stationCount = responseXML.text.trim
            logger.info(stationCount + " stations in bbox")
            if (stationCount.toDouble < stationBlockLimit) {
      //        logger.debug("prepending bbox with station count: " + stationCount)
              return bbox :: list
            }
            else
              return sliceBoundingBox(bbox, list)
        }
        case None => {
            return list
        }
      }
    }
    
    return List()
  }
  
  private def loadXMLFromString(stringToLoad : String) : Option[scala.xml.Elem] = {
    try {
      Some(scala.xml.XML.loadString(stringToLoad))
    } catch{
      case ex: Exception => {
          logger.error("Unable to load string into xml: " + stringToLoad + "\n" + ex.toString)
          None
      }
    }
  }
  
  private def fixResponseString(responseString : String) : String = {
    return responseString.replaceAll("""(&lt;)""", """<""").replaceAll("""(&gt;)""", """>""").replaceAll("""<\?xml version=[\"]1.0[\"] encoding=[\"]UTF-8[\"]\?>""", "").replaceAll("\n", "")
  }
  
  private def sliceBoundingBox(sliceBox : BoundingBox, boundingBoxList : List[BoundingBox]) : List[BoundingBox] = {
    // cut bounding box in half (along longitude)
//    logger.debug("slicing bbox: " + sliceBox.toString)
    // decide to slice bbox along lat/lon based on lists count
    if (boundingBoxList.length % 2 == 0) {
      val midLongitude = sliceBox.southWestCorner.getLongitude() + (sliceBox.northEastCorner.getLongitude() - sliceBox.southWestCorner.getLongitude()) / 2
      val bbox1 = new BoundingBox(
        new Location(sliceBox.southWestCorner.getLatitude(),sliceBox.southWestCorner.getLongitude()),
        new Location(sliceBox.northEastCorner.getLatitude(),midLongitude))
      val bbox2 = new BoundingBox(
        new Location(sliceBox.southWestCorner.getLatitude(),midLongitude),
        new Location(sliceBox.northEastCorner.getLatitude(), sliceBox.northEastCorner.getLongitude()))
      // send request for each bbox to see how many stations are in it
      val list1 = divideBBoxIntoChunks(bbox1, boundingBoxList)
      val list2 = divideBBoxIntoChunks(bbox2, boundingBoxList)
      return list1 ::: list2
    } else {
      val midLatitude = sliceBox.southWestCorner.getLatitude + (sliceBox.northEastCorner.getLatitude - sliceBox.southWestCorner.getLatitude) / 2
      val bbox1 = new BoundingBox(
        new Location(sliceBox.southWestCorner.getLatitude(),sliceBox.southWestCorner.getLongitude()),
        new Location(midLatitude,sliceBox.northEastCorner.getLongitude))
      val bbox2 = new BoundingBox(
        new Location(midLatitude,sliceBox.southWestCorner.getLongitude()),
        new Location(sliceBox.northEastCorner.getLatitude(), sliceBox.northEastCorner.getLongitude()))
      // send request for each bbox to see how many stations are in it
      val list1 = divideBBoxIntoChunks(bbox1, boundingBoxList)
      val list2 = divideBBoxIntoChunks(bbox2, boundingBoxList)
      return list1 ::: list2
    }
  }
}
  
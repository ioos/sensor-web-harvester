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
import com.axiomalaska.sos.source.Units
import com.axiomalaska.sos.source.data.SensorPhenomenonIds
import com.axiomalaska.sos.tools.HttpSender
import org.apache.log4j.Logger
import scala.xml.NodeSeq

case class StoretStation (stationId: String, stationName: String, lat: Double, lon: Double, orgId: String)

class StoretStationUpdater (private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  private val source = stationQuery.getSource(SourceId.STORET)
  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val httpSender = new HttpSender()
  
  private val SOAP = "http://schemas.xmlsoap.org/soap/envelope/"

  def update() {
    val sourceStationSensors = getSourceStations()

    val databaseStations = stationQuery.getStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }
  
  val name = "STORET"
  
  private def getSourceStations() : List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    // get a list of the bounding boxes to iterate
    val bboxList = getStationCount(boundingBox, List())
    var orgStationList:List[(String, List[String])] = Nil
    // get station info for our bounding box(es)
    val stationlist = for {
      (bbox, index) <- bboxList.zipWithIndex
    } yield {
      // get lat, lon, name, id, any description, and platform (if that is possible); create database station for each result
      logger.info("getting stations for bbox: " + bbox.toString)
      val xml = getStationsForMap(bbox)
      val stations = xml match {
        case Some(xml) => {
            val slist = for (val row <- xml \\ "Organization") yield {
             val sublist = for (val srow <- xml \\ "MonitoringLocation") yield {
                val stationId = (srow \\ "MonitoringLocationIdentifier").text
                val stationName = (srow \\ "MonitoringLocationName").text
                val lat = (srow \\ "LatitudeMeasure").text
                val lon = (srow \\ "LongitudeMeasure").text
                StoretStation(stationId, stationName, lat.toDouble, lon.toDouble, (row \ "OrganizationDescription" \ "OrganizationIdentifier").text)
                logger.info("adding station: " + stationId + ", " + stationName + ", " + lat + ", " + lon)
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
      
      stations.toList
   }
  val flatStationList = stationlist.flatten
   // have list of stations to iterate through
   if (flatStationList != Nil) {
     val size = flatStationList.length - 1
     val stationCollection = for {
       (station, index) <- flatStationList.zipWithIndex
       val sourceObservedProperties = getObservedProperties(station.stationId, station.orgId)
       val dbStation = new DatabaseStation(station.stationName, station.stationId, station.stationId, "", "Watershed", source.id, station.lat, station.lon)
       val databaseObservedProperties = stationUpdater.updateObservedProperties(source, sourceObservedProperties)
       val sensors = stationUpdater.getSourceSensors(dbStation, databaseObservedProperties)
       if (sensors.nonEmpty)
    } yield {
      logger.info("[" + index + " of " + size + "] station: " + station.stationName)
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
                  </srs:getResults>
                </soap:Body>
              </soap:Envelope>
    val response = httpSender.sendPostMessage("http://ofmpub.epa.gov/STORETwebservices/StoretResultService/", xml.toString)
    //    use a test file
//    var testFile = scala.io.Source.fromFile("ex_getResultsResponse.xml")
//    var response = testFile.mkString
//    testFile.close()
    if (response != null) {
      val responseFix = response.toString.replaceAll("""(&lt;)""", """<""").replaceAll("""(&gt;)""", """>""").replaceAll("""<\?xml version=[\"]1.0[\"] encoding=[\"]UTF-8[\"]\?>""", "").replaceAll("\n", "")
      val root = scala.xml.XML.loadString(responseFix)
      var charNameList:List[String] = Nil
      val results = for {
        node <- (root \\ "ResultDescription")
        if (node.nonEmpty)
        if (!((node \\ "ResultDetectionConditionText").exists(_.text.toLowerCase contains "non-detect") || (node \\ "ResultDetectionConditionText").exists(_.text.toLowerCase contains "present")))
        val name = (node \ "CharacteristicName").text
        if (!charNameList.exists(p => p == name))
      } yield {
        charNameList = name :: charNameList
        name
      }
      results.toList.flatMap(charName => getObservedProperty(charName))
    } else
      Nil
  }
  
  private def getObservedProperty(name: String) : Option[ObservedProperty] = {
    // condition on name as some of the names are stupidly long and difficult to parse
    val nameLower = name.toLowerCase
    if (nameLower contains "alkalinity") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.ALKALINITY_HCB))
    }
    else if (nameLower contains "ph") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.NONE, SensorPhenomenonIds.PH_WATER))
    }
    else if (nameLower contains "temperature") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.CELSIUS, SensorPhenomenonIds.FRESH_WATER_TEMPERATURE))
    }
    else if (nameLower contains "flow") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.CUBIC_FOOT_PER_SECOUND, SensorPhenomenonIds.STREAM_FLOW))
    }
    else if (nameLower contains "aluminum") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.ALUMINUM))
    }
    else if (nameLower contains "iron") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.IRON))
    }
    else if (nameLower contains "conductance") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICRO_MHOS_PER_CENTIMETERS, SensorPhenomenonIds.FRESH_WATER_CONDUCTIVITY))
    }
    else if (nameLower contains "oxygen") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.DISSOLVED_OXYGEN))
    }
    else if (nameLower contains "zinc") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.ZINC))
    }
    else {
      logger.info("[" + source.name + "] observed property: " + nameLower +
          " is not processed correctly.")
      None
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
   val response = httpSender.sendPostMessage("http://ofmpub.epa.gov/STORETwebservices/StationService/", xmlRequest.toString())
//   val testFile = scala.io.Source.fromFile("ex_getStationsForMapResponse.xml")
//   val response = testFile.mkString
//   testFile.close()
   if (response != null) {
     val responseFix = response.toString.replaceAll("""(&lt;)""", """<""").replaceAll("""(&gt;)""", """>""").replaceAll("""<\?xml version=[\"]1.0[\"] encoding=[\"]UTF-8[\"]\?>""", "").replaceAll("\n", "")
     Some(scala.xml.XML.loadString(responseFix.toString))
   }
   else
     None
  }
  
  private def getStationCount(bbox : BoundingBox, list : List[BoundingBox]) : List[BoundingBox] = {
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
    logger.info("sending request: " + xml.toString)
    val response = httpSender.sendPostMessage("http://ofmpub.epa.gov/STORETwebservices/StationService/", xml.toString())
    if (response != null ) {
      // can we treat response as xml?
      val responseXML = scala.xml.XML.loadString(response)
      logger.info("response: " + responseXML.toString)
      val stationCount = responseXML.text
      logger.info("bbox station count response: " + stationCount);
      if (stationCount.toDouble < 20000)
        return bbox :: list
      else
        return sliceBoundingBox(bbox, list)
    }
    
    return List()
  }
  
  private def sliceBoundingBox(sliceBox : BoundingBox, boundingBoxList : List[BoundingBox]) : List[BoundingBox] = {
    // cut bounding box in half (along longitude)
    logger.info("slicing bbox: " + sliceBox.toString)
    val midLongitude = sliceBox.southWestCorner.getLongitude() + (sliceBox.northEastCorner.getLongitude() - sliceBox.southWestCorner.getLongitude()) / 2
    val bbox1 = new BoundingBox(
      new Location(sliceBox.southWestCorner.getLatitude(),sliceBox.southWestCorner.getLongitude()),
      new Location(sliceBox.northEastCorner.getLatitude(),midLongitude))
    val bbox2 = new BoundingBox(
      new Location(sliceBox.southWestCorner.getLatitude(),midLongitude),
      new Location(sliceBox.northEastCorner.getLatitude(), sliceBox.northEastCorner.getLongitude()))
    // send request for each bbox to see how many stations are in it
    val list1 = getStationCount(bbox1, boundingBoxList)
    val list2 = getStationCount(bbox2, boundingBoxList)
    return list1 ::: list2
  }
}
  
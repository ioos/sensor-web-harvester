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

case class StoretStation (stationId: String, stationName: String, lat: Double, lon: Double, orgId: String)

class StoretStationUpdater (private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  private val source = stationQuery.getSource(SourceId.STORET)
  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val httpSender = new HttpSender()
  private val stationBlockLimit = 100
  
  private val resultURL = "http://ofmpub.epa.gov/STORETwebservices/StoretResultService/"
  private val stationURL = "http://ofmpub.epa.gov/STORETwebservices/StationService/"

  def update() {
    val bboxList = getStationCount(boundingBox, List())
    
    for (bbox <- bboxList) {
      val sourceStationSensors = getSourceStations(bbox)

      val databaseStations = stationQuery.getStations(source)

      stationUpdater.updateStations(sourceStationSensors, databaseStations)
    }
  }
  
  val name = "STORET"
  
  private def getSourceStations(bbox: BoundingBox) : List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    // get a list of the bounding boxes to iterate
//    val bboxList = getStationCount(boundingBox, List())
//    var orgStationList:List[(String, List[String])] = Nil
    // get station info for our bounding box(es)
//    val stationlist = for {
//      (bbox, index) <- bboxList.zipWithIndex
//    } yield {
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
      
//      stations.toList
//   }
    val flatStationList = stations.toList
   // have list of stations to iterate through
   if (flatStationList != Nil) {
     val size = flatStationList.length
     val stationCollection = for {
       (station, index) <- flatStationList.zipWithIndex
       val sourceObservedProperties = getObservedProperties(station.stationId, station.orgId)
       val dbStation = new DatabaseStation(station.stationName, station.stationId, station.stationId, "", "Watershed", source.id, station.lat, station.lon)
       val databaseObservedProperties = stationUpdater.updateObservedProperties(source, sourceObservedProperties)
       val sensors = stationUpdater.getSourceSensors(dbStation, databaseObservedProperties)
       if (sensors.nonEmpty)
    } yield {
      logger.info("[" + (index+1).toString + " of " + size + "] station: " + station.stationId)
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
//    var testFile = scala.io.Source.fromFile("ex_getResultsResponse.xml")
//    var response = testFile.mkString
//    testFile.close()
    if (response != null) {
      logger.debug("processing properties for " + stationId + " - " + orgId)
      val responseFix = response.toString.replaceAll("""(&lt;)""", """<""").replaceAll("""(&gt;)""", """>""").replaceAll("""<\?xml version=[\"]1.0[\"] encoding=[\"]UTF-8[\"]\?>""", "").replaceAll("\n", "")
      val root = scala.xml.XML.loadString(responseFix)
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
        name
      }
      results.toList.flatMap(property => getObservedProperty(property))
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
    else if (nameLower contains "carbon, total organic") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.NUTRIENT_NITROGEN))
    }
    else if (nameLower contains "nutrient-nitrogen") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.NUTRIENT_NITROGEN))
    }
    else if (nameLower contains "ph") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.NONE, SensorPhenomenonIds.PH_FRESH_WATER))
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
    else if ((nameLower contains "conductance") || (nameLower contains "conductivity")) {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICRO_MHOS_PER_CENTIMETERS, SensorPhenomenonIds.FRESH_WATER_CONDUCTIVITY))
    }
    else if (nameLower contains "hardness, carbonate") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.HARDNESS_CARBONATE))
    }
    else if (nameLower contains "mercury") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.MERCURY))
    }
    else if (nameLower contains "oxygen") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.DISSOLVED_OXYGEN))
    }
    else if (nameLower contains "zinc") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.ZINC))
    }
    else if (nameLower contains "vanadium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.VANADIUM))
    }
    else if (nameLower contains "chlorine") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.CHLORINE))
    }
    else if (nameLower contains "dysprosium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.DYSPROSIUM))
    }
    else if (nameLower contains "manganese") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.MANGANESE))
    }
    else if (nameLower contains "sodium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.SODIUM))
    }
    else if (nameLower contains "magnesium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.MAGNESIUM))
    }
    else if (nameLower contains "so4") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.SO4))
    }
    else if (nameLower contains "fluorine") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.FLUORINE))
    }
    else if (nameLower contains "fluoride") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.FLUORIDE))
    }
    else if (nameLower contains "bromide") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.BROMIDE))
    }
    else if (nameLower contains "bromine") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.BROMINE))
    }
    else if (nameLower contains "cadmium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.CADMIUM))
    }
    else if (nameLower contains "barium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.BARIUM))
    }
    else if (nameLower contains "boron") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.BORON))
    }
    else if (nameLower contains "titanium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.TITANIUM))
    }
    else if (nameLower contains "strontium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.STRONTIUM))
    }
    else if (nameLower contains "silicon") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.SILICON))
    }
    else if (nameLower contains "potassium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.POTASSIUM))
    }
    else if (nameLower contains "lead") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.LEAD))
    }
    else if (nameLower contains "molybdenum") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.MOLYBDENUM))
    }
    else if (nameLower contains "nickel") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.NICKEL))
    }
    else if (nameLower contains "phosphate-phosphorus") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.PHOSPHATE))
    }
    else if (nameLower contains "calcium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.CALCIUM))
    }
    else if (nameLower contains "fecal") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.PARTS_PER_100_MILILETER, SensorPhenomenonIds.FECAL_COLIFORM))
    }
    else if (nameLower contains "solids") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.TOTAL_SUSPENDED_SOLIDS))
    }
    else if (nameLower contains "turbidity") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.NEPHELOMETRIC_TURBIDITY_UNITS, SensorPhenomenonIds.WATER_TURBIDITY))
    }
    else if (nameLower contains "organic carbon") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.ORGANIC_CARBON))
    }
    else if (nameLower contains "carbon") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.CARBON))
    }
    else if (nameLower contains "nitrate") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MILLI_EQUIVILANTS_OF_SOLVENT_PER_LITER, SensorPhenomenonIds.NITRATE))
    }
    else if (nameLower contains "ammonia") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MILLI_EQUIVILANTS_OF_SOLVENT_PER_LITER, SensorPhenomenonIds.AMMONIA_NITROGEN))
    }
    else if (nameLower contains "sulfate") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MILLI_EQUIVILANTS_OF_SOLVENT_PER_LITER, SensorPhenomenonIds.CARBON))
    }
    else if (nameLower contains "chloride") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MILLI_EQUIVILANTS_OF_SOLVENT_PER_LITER, SensorPhenomenonIds.CHLORIDE))
    }
    else if (nameLower contains "silica") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.SILICA))
    }
    else if (nameLower contains "color") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.PCU, SensorPhenomenonIds.TRUE_COLOR))
    }
    else if (nameLower contains "rbp2, low g") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.NONE, SensorPhenomenonIds.RBP2_LOW_G))
    }
    else if (nameLower contains "bedrock") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.PERCENT, SensorPhenomenonIds.RBP2_SUBSTRATE_INORGANIC_BEDROCK))
    }
    else if (nameLower contains "cobble") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.PERCENT, SensorPhenomenonIds.RBP2_SUBSTRATE_INORGANIC_COBBLE))
    }
    else if (nameLower contains "boulder") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.PERCENT, SensorPhenomenonIds.RBP2_SUBSTRATE_INORGANIC_BOULDER))
    }
    else if (nameLower contains "clay") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.PERCENT, SensorPhenomenonIds.RBP2_SUBSTRATE_INORGANIC_CLAY))
    }
    else if (nameLower contains "gravel") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.PERCENT, SensorPhenomenonIds.RBP2_SUBSTRATE_INORGANIC_GRAVEL))
    }
    else if (nameLower contains "sand") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.PERCENT, SensorPhenomenonIds.RBP2_SUBSTRATE_INORGANIC_SAND))
    }
    else if (nameLower contains "silt") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.PERCENT, SensorPhenomenonIds.RBP2_SUBSTRATE_INORGANIC_SILT))
    }
    else if (nameLower contains "stream depth - run") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.INCHES, SensorPhenomenonIds.RBP_STREAM_DEPTH_RUN))
    }
    else if (nameLower contains "stream depth - pool") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.INCHES, SensorPhenomenonIds.RBP_STREAM_DEPTH_POOL))
    }
    else if (nameLower contains "stream depth - riffle") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.INCHES, SensorPhenomenonIds.RBP_STREAM_DEPTH_RIFFLE))
    }
    else if (nameLower contains "arsenic") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.ARSENIC))
    }
    else if (nameLower contains "cobalt") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.COBALT))
    }
    else if (nameLower contains "antimony") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.ANTIMONY))
    }
    else if (nameLower contains "chromium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.CHROMIUM))
    }
    else if (nameLower contains "beryllium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.BERYLLIUM))
    }
    else if (nameLower contains "copper") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.COPPER))
    }
    else if (nameLower contains "caco3") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.STD_UNITS, SensorPhenomenonIds.CACO3))
    }
    else if (nameLower contains "depth") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.INCHES, SensorPhenomenonIds.DEPTH_TO_WATER_BOTTOM))
    }
    else if (nameLower contains "width") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.INCHES, SensorPhenomenonIds.STREAM_WIDTH))
    }
    else if (nameLower contains "velocity") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.METER_PER_SECONDS, SensorPhenomenonIds.STREAM_VELOCITY))
    }
    else if (nameLower contains "kjeldahl") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.KJELDAHL_NITROGEN))
    }
    else if (nameLower contains "butyl") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.BUTYL_BENZYL_PHTHALATE))
    }
    else if (nameLower contains "inorganic nitrogen") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.INORGANIC_NITROGEN))
    }
    else if (nameLower contains "silver") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.SILVER))
    }
    else if (nameLower contains "uranium") {
      new Some[ObservedProperty](
        stationUpdater.createObservedProperty(name, source, Units.MICROGRAMS_PER_LITER, SensorPhenomenonIds.URANIUM_238))
    }
    else {
      logger.info("[" + source.name + "] observed property: " + nameLower +
          " was not processed correctly.")
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
   val response = httpSender.sendPostMessage(stationURL, xmlRequest.toString())
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
    val response = httpSender.sendPostMessage(stationURL, xml.toString())
    if (response != null ) {
      // can we treat response as xml?
      val responseXML = scala.xml.XML.loadString(response)
      val stationCount = responseXML.text
      if (stationCount.toDouble < stationBlockLimit) {
        logger.debug("prepending bbox with station count: " + stationCount)
        return bbox :: list
      }
      else
        return sliceBoundingBox(bbox, list)
    }
    
    return List()
  }
  
  private def sliceBoundingBox(sliceBox : BoundingBox, boundingBoxList : List[BoundingBox]) : List[BoundingBox] = {
    // cut bounding box in half (along longitude)
    logger.debug("slicing bbox: " + sliceBox.toString)
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
  
package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.BoundingBox
import org.apache.log4j.Logger
import org.w3c.dom.ls.DOMImplementationLS
import webservices2.RequestsServiceLocator
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.source.Units
import com.axiomalaska.sos.source.data.SensorPhenomenonIds

case class NerrsStation(siteId:String, stationCode:String, stationName:String, 
    latitude:Double, longitude:Double, isActive:Boolean, state:String, 
    reserveName:String, paramsReported:List[String] )
    
class NerrsStationUpdater(
  private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater  {
  
  private val source = stationQuery.getSource(SourceId.NERRS)
  private val geoTools = new GeoTools()
  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  
  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update() {
    val sourceStationSensors = getSourceStations()

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }
  
  val name = "NERRS"
    
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getSourceStations(): 
	  List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {

    val nerrsStations = createNerrsStations
    val size = nerrsStations.length - 1
    logger.info(nerrsStations.size + " stations unfiltered")
    val stationSensorsCollection = for {
      (nerrsStation, index) <- nerrsStations.zipWithIndex
      if(nerrsStation.isActive)
      if (withInBoundingBox(nerrsStation))
      val sourceObservedProperties = getSourceObservedProperties(nerrsStation)
      val databaseObservedProperties = 
        stationUpdater.updateObservedProperties(source, sourceObservedProperties)
      val station = createStation(nerrsStation)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if (sensors.nonEmpty)
    } yield {
      logger.debug("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }

    logger.info("Finished processing " + stationSensorsCollection.size + " stations")

    return stationSensorsCollection
  }
  
  private def getSourceObservedProperties(nerrsStation: NerrsStation) =
    nerrsStation.paramsReported.flatMap(matchObservedProperty)

  //WINDIR, , Sal, NO3F, TotSoRad, CLOUD, RH, TotPAR, ChlFluor, NO23F, TIDE, 
  //WINSPD, WAVHGT, Ke_N, PHOSH, NO2F, ATemp, CHLA_N, MaxWSpdT, UREA, Level, 
  //NH4F, TotPrcp, PRECIP, SpCond, MaxWSpd, PO4F, DO_mgl, pH, DO_pct, CumPrcp, 
  //Turb, BP, Depth, WSpd, SDWDir, Wdir, Temp
  private def matchObservedProperty(param: String): Option[ObservedProperty] = {
    param match {
      case "ATemp" => {
            getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_AVERAGE, param)
      }
      case "RH" => {
            getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY_AVERAGE, param)
      }
      case "BP" => {
            getObservedProperty(Phenomena.instance.AIR_PRESSURE, param)
      }
      case "WSpd" => {
            getObservedProperty(Phenomena.instance.WIND_SPEED, param)
      }
      case "Wdir" => {
            getObservedProperty(Phenomena.instance.WIND_FROM_DIRECTION, param)
      }
      case "MaxWSpd" => {
            getObservedProperty(Phenomena.instance.WIND_SPEED_OF_GUST, param)
      }
      case "TotPrcp" => {
            getObservedProperty(Phenomena.instance.PRECIPITATION_ACCUMULATED, param)
      }
      case "AvgVolt" => {
            getObservedProperty(Phenomena.instance.BATTERY_VOLTAGE, param)
      }
      case "CumPrcp" => {
            getObservedProperty(Phenomena.instance.PRECIPITATION_INCREMENT, param)
      }
      case "TotSoRad" => {
            getObservedProperty(Phenomena.instance.SOLAR_RADIATION, param)
      }
      case "Temp" => {
            getObservedProperty(Phenomena.instance.SEA_WATER_TEMPERATURE, param)
      }
      case "Sal" => {
            getObservedProperty(Phenomena.instance.SALINITY, param)
      }
      case "DO_pct" => {
            getObservedProperty(Phenomena.instance.DISSOLVED_OXYGEN_SATURATION, param)
      }
      case "DO_mgl" => {
            getObservedProperty(Phenomena.instance.DISSOLVED_OXYGEN, param)
      }
      case "pH" => {
            getObservedProperty(Phenomena.instance.SEA_WATER_PH_REPORTED_ON_TOTAL_SCALE, param)
      }
      case "Turb" => {
            getObservedProperty(Phenomena.instance.TURBIDITY, param)
      }
      case "ChlFluor" => {
            getObservedProperty(Phenomena.instance.CHLOROPHYLL_FLOURESCENCE, param)
      }
      case "PO4F" => {
            getObservedProperty(Phenomena.instance.PHOSPHATE, param)
      }
      case "NH4F" => {
            getObservedProperty(Phenomena.instance.AMMONIUM, param)
      }
      case "NO2F" => {
            getObservedProperty(Phenomena.instance.NITRITE, param)
      }
      case "NO3F" => {
            getObservedProperty(Phenomena.instance.NITRATE, param)
      }
      case "NO23F" => {
            getObservedProperty(Phenomena.instance.NITRITE_PLUS_NITRATE, param)
      }
      case "CHLA_N" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.MICROGRAMS_PER_LITER, 
            SensorPhenomenonIds.CHLOROPHYLL))
            getObservedProperty(Phenomena.instance.CHLOROPHYLL, param)
      }
      case "Depth" => None
      case "Level" => None
      
      case "MaxWSpdT" => None//Maximum Wind Speed Time hh:mm
      case "SDWDir" => None //Wind Direction Standard Deviation sd
      case "TotPAR" => None //Total PAR (LiCor) mmoles/m^2
      case "SpCond" => None//Specific Conductivity mS/cm 
      case "Ke_N" => None //not supported
      case "TIDE" => None //not supported
      case "WAVHGT" => None //not supported
      case "PHOSH" => None //not supported
      case "CLOUD" => None //not supported
      case "UREA" => None //not supported
      case "PRECIP" => None //not supported
      case "WINDIR" => None//not supported
      case "WINSPD" => None//not supported
      case "" => None
      case _ => {
        logger.debug("[" + source.name + "] observed property: " + param +
          " is not processed correctly.")
        None
      }
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
  
  private def withInBoundingBox(station: NerrsStation): Boolean = {
    val stationLocation = new Location(station.latitude, station.longitude)
    return geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }

  private def createStation(nerrsStation: NerrsStation): DatabaseStation = {
    new DatabaseStation(nerrsStation.stationName, nerrsStation.stationCode,
      nerrsStation.stationCode, nerrsStation.reserveName,
      "FIXED MET STATION", source.id, nerrsStation.latitude,
      nerrsStation.longitude)
  }

  private def createNerrsStations(): List[NerrsStation] = {
    nerrsStationXml match {
      case Some(xml) => {
        val nerrsStations = for (row <- (xml \\ "data")) yield {
          val siteId = (row \\ "NERR_Site_ID").text
          val stationCode = (row \\ "Station_Code").text
          val stationName = (row \\ "Station_Name").text
          val latitude = (row \\ "Latitude").text.toDouble
          val longitude = (row \\ "Longitude").text.toDouble
          val isActive = (row \\ "Status").text match {
            case "Active" => true
            case _ => false
          }
          val state = (row \\ "State").text
          val reserveName = (row \\ "Reserve_Name").text
          val paramsReported = (row \\ "Params_Reported").text.split(",").toList

          NerrsStation(siteId, stationCode, stationName, latitude, longitude,
            isActive, state, reserveName, paramsReported)
        }

        nerrsStations.toList
      }
      case None => Nil
    }
  }

  private def nerrsStationXml(): Option[scala.xml.Elem] = {
    try {
      val locator = new RequestsServiceLocator();

      val requests = locator.getRequestsCfc();

      val doc = requests.exportStationCodesXMLNew();

      val domImplLS = doc.getImplementation().asInstanceOf[DOMImplementationLS]

      val serializer = domImplLS.createLSSerializer();
      val str = serializer.writeToString(doc);

      Some(scala.xml.XML.loadString(str))
    } catch {
      case e: Exception => {
        None
      }
    }
  }
  
}
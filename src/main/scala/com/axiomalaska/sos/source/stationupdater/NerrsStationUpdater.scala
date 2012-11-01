package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.BoundingBox
import org.apache.log4j.Logger
import org.w3c.dom.ls.DOMImplementationLS
import webservices2.RequestsServiceLocator
import scala.xml.NodeSeq
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.source.GeoTools
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
    nerrsStation.paramsReported.flatMap(getObservedProperty)

  //WINDIR, , Sal, NO3F, TotSoRad, CLOUD, RH, TotPAR, ChlFluor, NO23F, TIDE, 
  //WINSPD, WAVHGT, Ke_N, PHOSH, NO2F, ATemp, CHLA_N, MaxWSpdT, UREA, Level, 
  //NH4F, TotPrcp, PRECIP, SpCond, MaxWSpd, PO4F, DO_mgl, pH, DO_pct, CumPrcp, 
  //Turb, BP, Depth, WSpd, SDWDir, Wdir, Temp
  private def getObservedProperty(param: String): Option[ObservedProperty] = {
    param match {
      case "ATemp" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.CELSIUS, SensorPhenomenonIds.AIR_TEMPERATURE_AVERAGE))
      }
      case "RH" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.PERCENT, SensorPhenomenonIds.RELATIVE_HUMIDITY_AVERAGE))
      }
      case "BP" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.MILLI_BAR, SensorPhenomenonIds.BAROMETRIC_PRESSURE))
      }
      case "WSpd" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.METER_PER_SECONDS, SensorPhenomenonIds.WIND_SPEED))
      }
      case "Wdir" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.DEGREES, SensorPhenomenonIds.WIND_DIRECTION))
      }
      case "MaxWSpd" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.METER_PER_SECONDS, SensorPhenomenonIds.WIND_GUST))
      }
      case "TotPrcp" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.MILLIMETERS, SensorPhenomenonIds.PRECIPITATION_ACCUMULATION))
      }
      case "AvgVolt" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.VOLTAGE, SensorPhenomenonIds.BATTERY))
      }
      case "CumPrcp" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.MILLIMETERS, SensorPhenomenonIds.PRECIPITATION_INCREMENT))
      }
      case "TotSoRad" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.WATT_PER_METER_SQUARED, SensorPhenomenonIds.SOLAR_RADIATION))
      }
      case "Temp" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.CELSIUS, SensorPhenomenonIds.SEA_WATER_TEMPERATURE))
      }
      case "Sal" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.PARTS_PER_TRILLION, SensorPhenomenonIds.SALINITY))
      }
      case "DO_pct" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.PERCENT, SensorPhenomenonIds.DISSOLVED_OXYGEN_SATURATION))
      }
      case "DO_mgl" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.MILLIGRAMS_PER_LITER, SensorPhenomenonIds.DISSOLVED_OXYGEN))
      }
      case "pH" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.STD_UNITS, SensorPhenomenonIds.PH_WATER))
      }
      case "Turb" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.NEPHELOMETRIC_TURBIDITY_UNITS, 
            SensorPhenomenonIds.WATER_TURBIDITY))
      }
      case "ChlFluor" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.MICROGRAMS_PER_LITER, 
            SensorPhenomenonIds.CHLOROPHYLL_FLUORESCENCE))
      }
      case "PO4F" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.MILLIGRAMS_PER_LITER, 
            SensorPhenomenonIds.ORTHOPHOSPHATE))
      }
      case "NH4F" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.MILLIGRAMS_PER_LITER, 
            SensorPhenomenonIds.AMMONIUM))
      }
      case "NO2F" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.MILLIGRAMS_PER_LITER, 
            SensorPhenomenonIds.NITRITE))
      }
      case "NO3F" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.MILLIGRAMS_PER_LITER, 
            SensorPhenomenonIds.NITRATE))
      }
      case "NO23F" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.MILLIGRAMS_PER_LITER, 
            SensorPhenomenonIds.NITRITE_PLUS_NITRATE))
      }
      case "CHLA_N" => {
        new Some[ObservedProperty](
          stationUpdater.createObservedProperty(param,
            source, Units.MICROGRAMS_PER_LITER, 
            SensorPhenomenonIds.CHLOROPHYLL))
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
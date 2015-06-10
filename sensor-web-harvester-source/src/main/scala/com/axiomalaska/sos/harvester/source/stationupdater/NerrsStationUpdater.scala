package com.axiomalaska.sos.harvester.source.stationupdater

import com.axiomalaska.sos.harvester.BoundingBox;
import com.axiomalaska.sos.harvester.GeoTools;
import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.Units;
import com.axiomalaska.sos.harvester.data.DatabasePhenomenon;
import com.axiomalaska.sos.harvester.data.DatabaseSensor;
import com.axiomalaska.sos.harvester.data.DatabaseStation;
import com.axiomalaska.sos.harvester.data.ObservedProperty;
import com.axiomalaska.sos.harvester.data.SourceId;

import org.apache.log4j.Logger
import org.w3c.dom.ls.DOMImplementationLS
import webservices2.RequestsServiceLocator
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.ioos.sos.GeomHelper
import org.joda.time.DateTime
import com.axiomalaska.ioos.sos.GeomHelper

case class NerrsStation(siteId: String, stationCode: String, stationName: String,
                        latitude: Double, longitude: Double, isActive: Boolean, state: String,
                        reserveName: String, paramsReported: List[String], time_begin: DateTime,
                        time_end: DateTime)

/**
 * The user of this code needs to register their IP address with NERRS to be able
 * to access the data. 
 */
class NerrsStationUpdater(
    private val stationQuery: StationQuery,
    private val boundingBox: BoundingBox) extends StationUpdater {

  private val source = stationQuery.getSource(SourceId.NERRS)
  private val geoTools = new GeoTools()
  private val stationUpdater = new StationUpdateTool(stationQuery)
  private val LOGGER = Logger.getLogger(getClass())

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

  private def getSourceStations(): List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {

    val nerrsStations = createNerrsStations
    val size = nerrsStations.length - 1
    LOGGER.info(nerrsStations.size + " stations unfiltered")
    val stationSensorsCollection = for {
      (nerrsStation, index) <- nerrsStations.zipWithIndex
      if (nerrsStation.isActive)
      val location = GeomHelper.createLatLngPoint(nerrsStation.latitude, nerrsStation.longitude)
      if (geoTools.isStationWithinRegion(location, boundingBox))
      val sourceObservedProperties = getSourceObservedProperties(nerrsStation)
      val databaseObservedProperties = stationUpdater.updateObservedProperties(source, sourceObservedProperties)
      val station = createStation(nerrsStation)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if (sensors.nonEmpty)
    } yield {
      LOGGER.debug("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }

    LOGGER.info("Finished processing " + stationSensorsCollection.size + " stations")

    return stationSensorsCollection
  }

  private def getSourceObservedProperties(nerrsStation: NerrsStation) =
    nerrsStation.paramsReported.flatMap(matchObservedProperty)

  //WINDIR, , Sal, NO3F, TotSoRad, CLOUD, RH, TotPAR, ChlFluor, NO23F, TIDE, 
  //WINSPD, WAVHGT, Ke_N, PHOSH, NO2F, ATemp, CHLA_N, MaxWSpdT, UREA, Level, 
  //NH4F, TotPrcp, PRECIP, SpCond, MaxWSpd, PO4F, DO_mgl, pH, DO_pct, CumPrcp, 
  //Turb, BP, Depth, WSpd, SDWDir, Wdir, Temp
  /**
   * This method is used to create an observedProperty from the source's value parameter tag.
   * The observedProperty metadata about the values that are coming in from the source
   * data provider. The units may not be the same as the associated Phenomenon and
   * the value will need to be converted.
   */
  private def matchObservedProperty(param: String): Option[ObservedProperty] = {
    param match {
      case "ATemp" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_AVERAGE,
          param, Units.CELSIUS, source))
      }
      case "RH" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY_AVERAGE,
          param, Units.PERCENT, source))
      }
      case "BP" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_PRESSURE, param,
          Units.MILLI_BAR, source))
      }
      case "WSpd" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.WIND_SPEED, param,
          Units.METER_PER_SECONDS, source))
      }
      case "Wdir" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.WIND_FROM_DIRECTION, param,
          Units.DEGREES, source))
      }
      case "MaxWSpd" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.WIND_SPEED_OF_GUST, param,
          Units.METER_PER_SECONDS, source))
      }
      case "TotPrcp" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.PRECIPITATION_ACCUMULATED,
          param, Units.MILLIMETERS, source))
      }
      case "AvgVolt" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.BATTERY_VOLTAGE, param,
          Units.VOLTAGE, source))
      }
      case "CumPrcp" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.PRECIPITATION_INCREMENT,
          param, Units.MILLIMETERS, source))
      }
      case "TotSoRad" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOLAR_RADIATION, param,
          Units.WATT_PER_METER_SQUARED, source))
      }
      case "Temp" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_WATER_TEMPERATURE,
          param, Units.CELSIUS, source))
      }
      case "Sal" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SALINITY, param,
          Units.PARTS_PER_TRILLION, source))
      }
      case "DO_pct" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.DISSOLVED_OXYGEN_SATURATION,
          param, Units.PERCENT, source))
      }
      case "DO_mgl" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.DISSOLVED_OXYGEN, param,
          Units.MILLIGRAMS_PER_LITER, source))
      }
      case "pH" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_WATER_PH_REPORTED_ON_TOTAL_SCALE,
          param, Units.STD_UNITS, source))
      }
      case "Turb" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.TURBIDITY, param,
          Units.NEPHELOMETRIC_TURBIDITY_UNITS, source))
      }
      case "ChlFluor" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.CHLOROPHYLL_FLOURESCENCE,
          param, Units.MICROGRAMS_PER_LITER, source))
      }
      case "PO4F" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.PHOSPHATE, param,
          Units.MILLIGRAMS_PER_LITER, source))
      }
      case "NH4F" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AMMONIUM, param,
          Units.MILLIGRAMS_PER_LITER, source))
      }
      case "NO2F" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.NITRITE, param,
          Units.MILLIGRAMS_PER_LITER, source))
      }
      case "NO3F" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.NITRATE, param,
          Units.MILLIGRAMS_PER_LITER, source))
      }
      case "NO23F" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.NITRITE_PLUS_NITRATE,
          param, Units.MILLIGRAMS_PER_LITER, source))
      }
      case "CHLA_N" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.CHLOROPHYLL, param,
          Units.MICROGRAMS_PER_LITER, source))
      }
      case "Depth"    => None
      case "Level"    => None

      case "MaxWSpdT" => None //Maximum Wind Speed Time hh:mm
      case "SDWDir"   => None //Wind Direction Standard Deviation sd
      case "TotPAR"   => None //Total PAR (LiCor) mmoles/m^2
      case "SpCond"   => None //Specific Conductivity mS/cm 
      case "Ke_N"     => None //not supported
      case "TIDE"     => None //not supported
      case "WAVHGT"   => None //not supported
      case "PHOSH"    => None //not supported
      case "CLOUD"    => None //not supported
      case "UREA"     => None //not supported
      case "PRECIP"   => None //not supported
      case "WINDIR"   => None //not supported
      case "WINSPD"   => None //not supported
      case ""         => None
      case _ => {
        LOGGER.debug("[" + source.name + "] observed property: " + param +
          " is not processed correctly.")
        None
      }
    }
  }

  private def createStation(nerrsStation: NerrsStation): DatabaseStation = {
    new DatabaseStation(nerrsStation.stationName,
      source.tag + ":" + nerrsStation.stationCode,
      nerrsStation.stationCode, nerrsStation.reserveName,
      "FIXED MET STATION", source.id, nerrsStation.latitude,
      nerrsStation.longitude, null, null)
  }

  private def createNerrsStations(): List[NerrsStation] = {
    nerrsStationXml match {
      case Some(xml) => {
        val nerrsStations = for (row <- (xml \\ "data")) yield {
          val latitudeRaw = (row \\ "Latitude").text
          val longitudeRaw = (row \\ "Longitude").text
          
          println(latitudeRaw + " " + longitudeRaw)

          if (latitudeRaw.nonEmpty && longitudeRaw.nonEmpty) {
            val siteId = (row \\ "NERR_Site_ID").text
            val stationCode = (row \\ "Station_Code").text
            val stationName = (row \\ "Station_Name").text
            val latitude = latitudeRaw.toDouble
            val longitude = longitudeRaw.toDouble * (-1)
            val isActive = (row \\ "Status").text match {
              case "Active" => true
              case _        => false
            }
            val state = (row \\ "State").text
            val reserveName = (row \\ "Reserve_Name").text
            val paramsReported = (row \\ "Params_Reported").text.split(",").toList

            Some(NerrsStation(siteId, stationCode, stationName, latitude, longitude,
              isActive, state, reserveName, paramsReported, null, null))
          } else {
            None
          }
        }

        nerrsStations.toList.flatten
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

package com.axiomalaska.sos.harvester.source.observationretriever

import org.apache.log4j.Logger
import org.joda.time.DateTime

import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.ObservationValues

object SosObservationRetriever {
  /**
   * The SOS that we are pulling the data from has different Phenomenon URLs than the SOS
   * we are placing the data into. For example the pulling data SOS has one URL for
   * wind where the SOS we are pushing data into has three wind_speed, wind_direction, and wind_gust
   */
  def getSensorForeignId(phenomenon: Phenomenon): String = {
    val localPhenomenon = phenomenon.asInstanceOf[LocalPhenomenon]

    if (localPhenomenon.getTag == Phenomena.instance.AIR_PRESSURE.getTag) {
      "http://mmisw.org/ont/cf/parameter/air_pressure"
    } else if (localPhenomenon.getTag == Phenomena.instance.AIR_TEMPERATURE.getTag) {
      "http://mmisw.org/ont/cf/parameter/air_temperature"
    } else if (localPhenomenon.getTag == Phenomena.instance.SEA_WATER_TEMPERATURE.getTag) {
      "http://mmisw.org/ont/cf/parameter/sea_water_temperature"
    } else if (localPhenomenon.getTag == Phenomena.instance.CURRENT_DIRECTION.getTag ||
      localPhenomenon.getTag == Phenomena.instance.CURRENT_SPEED.getTag) {
      "http://mmisw.org/ont/cf/parameter/currents"
    } else if (localPhenomenon.getTag == Phenomena.instance.SEA_SURFACE_HEIGHT_ABOVE_SEA_LEVEL.getTag) {
      "http://mmisw.org/ont/cf/parameter/water_surface_height_above_reference_datum"
    } else if (localPhenomenon.getTag ==
      Phenomena.instance.SEA_SURFACE_HEIGHT_AMPLITUDE_DUE_TO_GEOCENTRIC_OCEAN_TIDE.getTag) {
      "http://mmisw.org/ont/cf/parameter/sea_surface_height_amplitude_due_to_equilibrium_ocean_tide"
    } else if (localPhenomenon.getTag == Phenomena.instance.WIND_FROM_DIRECTION.getTag ||
      localPhenomenon.getTag == Phenomena.instance.WIND_SPEED_OF_GUST.getTag ||
      localPhenomenon.getTag == Phenomena.instance.WIND_SPEED.getTag ||
      localPhenomenon.getTag == Phenomena.instance.WIND_GUST_FROM_DIRECTION.getTag ||
      localPhenomenon.getTag == Phenomena.instance.WIND_VERTICAL_VELOCITY.getTag) {
      "http://mmisw.org/ont/cf/parameter/winds"
    } else if (localPhenomenon.getTag == Phenomena.instance.SALINITY.getTag()) {
      "http://mmisw.org/ont/cf/parameter/sea_water_salinity"
    } else if (localPhenomenon.getTag == Phenomena.instance.SEA_SURFACE_SWELL_WAVE_TO_DIRECTION.getTag ||
      localPhenomenon.getTag == Phenomena.instance.SEA_SURFACE_WIND_WAVE_TO_DIRECTION.getTag ||
      localPhenomenon.getTag == Phenomena.instance.SEA_SURFACE_WIND_WAVE_PERIOD.getTag ||
      localPhenomenon.getTag == Phenomena.instance.SEA_SURFACE_WIND_WAVE_SIGNIFICANT_HEIGHT.getTag ||
      localPhenomenon.getTag == Phenomena.instance.SEA_SURFACE_SWELL_WAVE_PERIOD.getTag ||
      localPhenomenon.getTag == Phenomena.instance.SEA_SURFACE_SWELL_WAVE_SIGNIFICANT_HEIGHT.getTag ||
      localPhenomenon.getTag == Phenomena.instance.SEA_SURFACE_SWELL_WAVE_TO_DIRECTION.getTag ||
      localPhenomenon.getTag == Phenomena.instance.SEA_SURFACE_DOMINANT_WAVE_TO_DIRECTION.getTag ||
      localPhenomenon.getTag == Phenomena.instance.DOMINANT_WAVE_PERIOD.getTag ||
      localPhenomenon.getTag == Phenomena.instance.SIGNIFICANT_WAVE_HEIGHT.getTag ||
      localPhenomenon.getTag == Phenomena.instance.MEAN_WAVE_PERIOD.getTag) {
      "http://mmisw.org/ont/cf/parameter/waves"
    } else {
      throw new Exception("Sensor Foreign Id not found: " + phenomenon.getTag())
    }
  }

}

abstract class SosObservationRetriever(private val stationQuery:StationQuery) 
	extends ObservationValuesCollectionRetriever {

import SosObservationRetriever._
  
  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val LOGGER = Logger.getLogger(getClass())
  private val sosRawDataRetriever = new SosRawDataRetriever()
  
  protected val serviceUrl:String

  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------

  def getObservationValues(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon, startDate: DateTime): List[ObservationValues] = {

    val sensorForeignId = sosRawDataRetriever.getSensorForeignId(phenomenon)

    val rawData = sosRawDataRetriever.getRawData(serviceUrl, 
        sensorForeignId, station.databaseStation.foreign_tag, startDate, DateTime.now())

    val observationValuesCollection = createSensorObservationValuesCollection(
        station, sensor, phenomenon)
    val phenomenonForeignTags = observationValuesCollection.map(_.observedProperty.foreign_tag)

    for {
      rawValue <- sosRawDataRetriever.createRawValues(rawData, phenomenonForeignTags)
      observationValues <- observationValuesCollection.find(
          _.observedProperty.foreign_tag == rawValue.phenomenonForeignTag)
      (dateTime, value) <- rawValue.values
      if (dateTime.isAfter(startDate))
    } {
      observationValues.addValue(value, dateTime)
    }

    observationValuesCollection
  }

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------
  
  private def createSensorObservationValuesCollection(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    for (observedProperty <- observedProperties) yield {
      new ObservationValues(observedProperty, sensor, phenomenon, observedProperty.foreign_units)
    }
  }
}


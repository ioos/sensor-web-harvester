package com.axiomalaska.sos.harvester.source.observationretriever

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.TimeZone
import scala.Array.canBuildFrom
import scala.Option.option2Iterable
import scala.collection.JavaConversions.seqAsJavaList
import org.apache.log4j.Logger
import org.cuahsi.waterML.x11.TimeSeriesResponseDocument
import org.cuahsi.waterML.x11.TsValuesSingleVariableType
import org.cuahsi.waterML.x11.ValueSingleVariable
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.axiomalaska.sos.harvester.StationQuery
import com.axiomalaska.sos.harvester.data.ObservedProperty
import com.axiomalaska.sos.harvester.source.SourceUrls
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.ObservationValues
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender
import org.cuahsi.waterML.x11.TimeSeriesType
import com.axiomalaska.sos.harvester.data.RawValues
import scala.collection.mutable

/**
 * Information on how to use the USGS water service
 * http://waterservices.usgs.gov/rest/IV-Service.html
 */
class UsgsWaterObservationRetriever(private val stationQuery: StationQuery,
  private val logger: Logger = Logger.getRootLogger())
  extends ObservationValuesCollectionRetriever {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------

  private val formatDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------

  def getObservationValues(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon, startDate: DateTime): List[ObservationValues] = {

    logger.info("USGS-WATER: Collecting for station - " + station.databaseStation.foreign_tag)

    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    val rawData = getRawData(station, sensor, phenomenon, startDate)
    if (rawData != null) {
      val observationValuesCollections =
        createSensorObservationValuesCollection(station, sensor, phenomenon)

      val phenomenonForeignTags = observationValuesCollections.map(_.observedProperty.foreign_tag)

      for {
        rawValues <- UsgsWaterObservationRetriever.
          getRawValuesForSingleStation(rawData, phenomenonForeignTags)
        observationValues <- observationValuesCollections.find(
          _.observedProperty.foreign_tag == rawValues.phenomenonForeignTag)
        (dateTime, value) <- rawValues.values
        if (dateTime.isAfter(startDate))
      } {
        observationValues.addValue(value.toDouble, dateTime)
      }
      
      observationValuesCollections
    } else {
      Nil
    }
  }

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def createSensorObservationValuesCollection(station: LocalStation, 
      sensor: LocalSensor, phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    for (observedProperty <- observedProperties) yield {
      new ObservationValues(observedProperty, sensor, phenomenon, observedProperty.foreign_units)
    }
  }
  
  def getRawData(station: LocalStation,
    sensor: LocalSensor, phenomenon: LocalPhenomenon, startDate: DateTime): String = {

    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    val parameterCd = observedProperties.map(_.foreign_tag).mkString(",")

    UsgsWaterObservationRetriever.getRawData(
        List(station.databaseStation.foreign_tag), parameterCd, startDate, DateTime.now())
  }
}

object UsgsWaterObservationRetriever {
  private val formatDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

  /**
   * returns - a list of station foreign IDs and a collection of RawValues for each station
   */
  def getRawValuesForMultipleStations(rawData: String): Map[String, List[RawValues]] = {
    val document = TimeSeriesResponseDocument.Factory.parse(rawData)
    val stationRawValuesMap = new mutable.HashMap[String, List[RawValues]]

    for {
      timeSeriesType <- document.getTimeSeriesResponse().getTimeSeriesArray()
      if (timeSeriesType.getValuesArray().nonEmpty)
      val (stationForeignId, rawValues) = getRawValues(timeSeriesType)
    } {
      stationRawValuesMap.get(stationForeignId) match {
        case Some(rawValuesCollection) => {
          stationRawValuesMap.put(stationForeignId, rawValues :: rawValuesCollection)
        }
        case None => {
          stationRawValuesMap.put(stationForeignId, List(rawValues))
        }
      }
    }

    stationRawValuesMap.toMap
  }

  def getRawValuesForSingleStation(rawData: String,
    phenomenonForeignTags: List[String]): List[RawValues] = {
    val document = TimeSeriesResponseDocument.Factory.parse(rawData)

    (for {
      timeSeriesType <- document.getTimeSeriesResponse().getTimeSeriesArray()
      if (timeSeriesType.getValuesArray().nonEmpty)
      val phenomonenForeignId = getPhenomonenForeignId(timeSeriesType)
      if (phenomenonForeignTags.isEmpty || phenomenonForeignTags.exists(_ == phenomonenForeignId))
      val (stationForeignId, rawValues) = getRawValues(timeSeriesType)
    } yield { rawValues }).toList
  }

  /**
   *
   * @stateId - the two character representing a state required eg. AK,CA
   */
  def getStateRawData(stateId: String,
    startDate: DateTime, endDate: DateTime): String = {
    val thrityDayBeforeEndDate = endDate.minusDays(30)

    val (formatedStartDate, formatedEndDate) =
      if (startDate.isBefore(thrityDayBeforeEndDate)) {
        (formatDate.format(getDateObjectInGMT(thrityDayBeforeEndDate)),
          formatDate.format(getDateObjectInGMT(endDate)))
      } else {
        (formatDate.format(getDateObjectInGMT(startDate)),
          formatDate.format(getDateObjectInGMT(endDate)))
      }

    val parts = List(
      new HttpPart("stateCd", stateId),
      new HttpPart("parameterCd", "all"),
      new HttpPart("startDT", formatedStartDate),
      new HttpPart("endDT", formatedEndDate),
      new HttpPart("siteStatus", "active"))

    HttpSender.sendGetMessage(SourceUrls.USGS_WATER_OBSERVATION_RETRIEVAL, parts)
  }
  
  /**
   * @stationForeignIds - the stations foreign IDs to pull the values from. 
   * Maximum of 100 stations. 
   */
  def getRawData(stationForeignIds: List[String],
    startDate: DateTime, endDate: DateTime): String = {

    val thrityDayBeforeEndDate = endDate.minusDays(30)

    val (formatedStartDate, formatedEndDate) =
      if (startDate.isBefore(thrityDayBeforeEndDate)) {
        (formatDate.format(getDateObjectInGMT(thrityDayBeforeEndDate)),
          formatDate.format(getDateObjectInGMT(endDate)))
      } else {
        (formatDate.format(getDateObjectInGMT(startDate)),
          formatDate.format(getDateObjectInGMT(endDate)))
      }

    val parts = List(
      new HttpPart("sites", stationForeignIds.mkString(",")),
      new HttpPart("startDT", formatedStartDate),
      new HttpPart("endDT", formatedEndDate),
      new HttpPart("siteStatus", "active"))

    HttpSender.sendGetMessage(SourceUrls.USGS_WATER_OBSERVATION_RETRIEVAL, parts)
  }
  
  /**
   * The modifiedSince tag only retrieves values that have change in the time requested.
   * This is for corrected values. 
   * 
   * @stationForeignIds - the stations foreign IDs to pull the values from. 
   * Maximum of 100 stations.
   * @numberOfHoursSinceModified - .  
   * @peroidToLookBack - how far to look back for changes
   */
  def getRawDataCorrectedValues(stationForeignIds: List[String], 
      numberOfHoursSinceModified: Int, peroidToLookBack:Int): String = {
    val modifiedSince = "PT" + numberOfHoursSinceModified + "H"
    val period = "PT" + peroidToLookBack + "H"
    val parts = List[HttpPart](
      new HttpPart("sites", stationForeignIds.mkString(",")),
      new HttpPart("modifiedSince", modifiedSince),
      new HttpPart("period", period),
      new HttpPart("siteStatus", "active"))

    HttpSender.sendGetMessage(SourceUrls.USGS_WATER_OBSERVATION_RETRIEVAL, parts, false)
  }

  /**
   *
   * @stations - the stations to pull the values from.
   */
  def getRawData(stationForeignIds: List[String], parameterCd: String,
    startDate: DateTime, endDate: DateTime): String = {

    val thrityDayBeforeEndDate = endDate.minusDays(30)

    val (formatedStartDate, formatedEndDate) =
      if (startDate.isBefore(thrityDayBeforeEndDate)) {
        (formatDate.format(getDateObjectInGMT(thrityDayBeforeEndDate)),
          formatDate.format(getDateObjectInGMT(endDate)))
      } else {
        (formatDate.format(getDateObjectInGMT(startDate)),
          formatDate.format(getDateObjectInGMT(endDate)))
      }

    val parts = List(
      new HttpPart("sites", stationForeignIds.mkString(",")),
      new HttpPart("parameterCd", parameterCd),
      new HttpPart("startDT", formatedStartDate),
      new HttpPart("endDT", formatedEndDate),
      new HttpPart("siteStatus", "active"))

    HttpSender.sendGetMessage(SourceUrls.USGS_WATER_OBSERVATION_RETRIEVAL, parts)
  }

  private def getRawValues(timeSeriesType: TimeSeriesType): (String, RawValues) = {
    val phenomonenForeignId = getPhenomonenForeignId(timeSeriesType)
    val noDataValue = getNoDataValue(timeSeriesType)
    val stationForeignId = getStationForeignId(timeSeriesType)
    val values = collectionValues(timeSeriesType.getValuesArray().head, noDataValue)

    (stationForeignId, RawValues(phenomonenForeignId, values))
  }

  private def getStationForeignId(timeSeriesType: TimeSeriesType): String = {
    val sourceInfo = timeSeriesType.getSourceInfo()
    val xmlResult = scala.xml.XML.loadString(sourceInfo.xmlText())
    (xmlResult \ "siteCode").head.text
  }

  private def collectionValues(
    tsValuesSingleVariableType: TsValuesSingleVariableType,
    noDataValue: Double): List[(DateTime, Double)] = {

    val rawValues = for {
      valueSingleVariable <- tsValuesSingleVariableType.getValueArray()
      val dateTime = createDate(valueSingleVariable)
      val value = valueSingleVariable.getBigDecimalValue().doubleValue()
      if (noDataValue != value)
    } yield {
      (dateTime, value)
    }

    rawValues.toList
  }

  private def getPhenomonenForeignId(timeSeriesTypes: TimeSeriesType): String = {
    timeSeriesTypes.getVariable().getVariableCodeArray(0).getStringValue()
  }

  private def getNoDataValue(timeSeriesTypes: TimeSeriesType): Double = {
    timeSeriesTypes.getVariable().getNoDataValue()
  }

  private def createDate(valueSingleVariable: ValueSingleVariable): DateTime = {
    val date = valueSingleVariable.getDateTime()
    date.setTimeZone(TimeZone.getTimeZone("UTC"))
    new DateTime(date)
  }

  private def getDateObjectInGMT(dateTime: DateTime): Date = {
    getDateObjectInGMT(dateTime.toCalendar(null))
  }
  
  private def getDateObjectInGMT(calendar: Calendar): Date = {
    val copyCalendar = calendar.clone().asInstanceOf[Calendar]
    copyCalendar.setTimeZone(TimeZone.getTimeZone("GMT"))
    val localCalendar = Calendar.getInstance()
    localCalendar.set(Calendar.YEAR, copyCalendar.get(Calendar.YEAR))
    localCalendar.set(Calendar.MONTH, copyCalendar.get(Calendar.MONTH))
    localCalendar.set(Calendar.DAY_OF_MONTH, copyCalendar.get(Calendar.DAY_OF_MONTH))
    localCalendar.set(Calendar.HOUR_OF_DAY, copyCalendar.get(Calendar.HOUR_OF_DAY))
    localCalendar.set(Calendar.MINUTE, copyCalendar.get(Calendar.MINUTE))
    localCalendar.set(Calendar.SECOND, copyCalendar.get(Calendar.SECOND))

    // The time is not able to be changed from the 
    //setTimezone if this is not set. Java Error
    calendar.getTime()

    localCalendar.getTime()
  }
}

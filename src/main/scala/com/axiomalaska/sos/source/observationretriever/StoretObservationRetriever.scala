/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.tools.HttpSender
import java.util.Calendar
import org.apache.log4j.Logger
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.source.data.ObservationValues
import java.text.SimpleDateFormat

class StoretObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger())
	extends ObservationValuesCollectionRetriever {
    
    private val resultURL = "http://ofmpub.epa.gov/STORETwebservices/StoretResultService/"
    private val httpSender = new HttpSender()
    private val dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    
    def getObservationValues(station: LocalStation, sensor: LocalSensor, 
      phenomenon: LocalPhenomenon, startDate: Calendar):List[ObservationValues] = {
      val observationValuesCollection = createSensorObservationValuesCollection(station, sensor, phenomenon)
      // request the result for each station using its foreign_tag, description (which is the organization id) and the phenomenon name
      val xml = getResults(station.databaseStation.foreign_tag, station.databaseStation.description, phenomenon.databasePhenomenon.name)
      xml match {
        case Some(xml) => {
            // iterate through each activity to get the date, value combo
            for (activity <- (xml \\ "Activity")) {
              val rawDate = (activity \\ "ActivityStartDate").text
              val rawTime = (activity \\ "ActivityStartTime").text
              val calendarDate = parseDateString(rawDate + " " + rawTime)
              if (calendarDate.after(startDate)) {
                for (result <- (activity \\ "Result")) {
                  val measuredValue = (result \\ "ResultMeasureValue").text.toDouble
                  for {
                    observationValue <- observationValuesCollection
                  }{
                    logger.info("adding - " + measuredValue + ", " + calendarDate.toString)
                    if(!observationValue.containsDate(calendarDate)) {
                      observationValue.addValue(measuredValue, calendarDate)
                    }
                  }
                }
              }
            }
            observationValuesCollection.filter(_.getValues.size > 0)
        }
        case None => {
            Nil
        }
      }
    }
    
    private def getResults(stationId : String, orgId : String, phenomenonName : String) : Option[scala.xml.Elem] = {
      val xmlRequest = <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:srs="http://storetresultservice.storet.epa.gov/">
                <soap:Body>
                  <srs:getResults>
                    <OrganizationId>{orgId}</OrganizationId>
                    <MonitoringLocationId>{stationId}</MonitoringLocationId>
                    <CharacteristicName>{phenomenonName}</CharacteristicName>
                    <MonitoringLocationType/><MinimumActivityStartDate/><MaximumActivityStartDate/><MinimumLatitude/><MaximumLatitude/><MinimumLongitude/><MaximumLongitude/><CharacteristicType/><ResultType/>
                  </srs:getResults>
                </soap:Body>
              </soap:Envelope>
//      val response = httpSender.sendPostMessage(resultURL, xmlRequest.toString)
    // read a file for now
    var response = ""
    if (phenomenonName.equalsIgnoreCase("iron")) {
      val file = scala.io.Source.fromFile("ex_getResultsSinglePhenomenon_Iron.xml")
      response = file.mkString
      file.close()
    } else {
      val file = scala.io.Source.fromFile("ex_getResultsSinglePhenomenon.xml")
      response = file.mkString
      file.close()
    }
    if (response != null) {
      val responseFix = response.toString.replaceAll("""(&lt;)""", """<""").replaceAll("""(&gt;)""", """>""").replaceAll("""<\?xml version=[\"]1.0[\"] encoding=[\"]UTF-8[\"]\?>""", "").replaceAll("\n", "")
      Some(scala.xml.XML.loadString(responseFix.toString))
    }
    else
      None
  }
    
  private def createSensorObservationValuesCollection(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    for (observedProperty <- observedProperties) yield {
      new ObservationValues(observedProperty, sensor, phenomenon, observedProperty.foreign_units)
    }
  }
  
  private def parseDateString(rawString : String) : Calendar = {
    val date = dateParser.parse(rawString)
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(date.getTime)
    
    calendar.getTime()
    
    return calendar
  }
}
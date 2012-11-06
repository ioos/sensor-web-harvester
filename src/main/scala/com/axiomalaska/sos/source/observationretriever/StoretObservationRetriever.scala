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

case class StoretResponse (stationId: String, requestDate: Calendar, obsList: List[(String, List[(Calendar, Double)])])

object StoretObservationRetriever {
  private var storedStationRequests: List[StoretResponse] = Nil
}

class StoretObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger())
	extends ObservationValuesCollectionRetriever {
    import StoretObservationRetriever._
          
    private val resultURL = "http://ofmpub.epa.gov/STORETwebservices/StoretResultService/"
    private val httpSender = new HttpSender()
    private val dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    
    def getObservationValues(station: LocalStation, sensor: LocalSensor, 
      phenomenon: LocalPhenomenon, startDate: Calendar):List[ObservationValues] = {

      logger.info("STORET: Collecting for station - " + station.databaseStation.foreign_tag + " - observation - " + phenomenon.databasePhenomenon.tag)
      
      // check to see if the desired station is already in the list
      var stationItems: List[StoretResponse] = storedStationRequests.filter(p => p.stationId.equalsIgnoreCase(station.databaseStation.foreign_tag))
      if (stationItems.nonEmpty) {
        // there is at least 1 item
        val timeConstrainedList = stationItems.filter(p => ( p.requestDate.before(startDate) || p.requestDate.equals(startDate) ) )
        if (timeConstrainedList.nonEmpty) {
          // we have a stored station response with a time constraint at least equal to the start date
          stationItems = timeConstrainedList
        } else {
          // stored station item that is later than the start date, remove it and do a request
          storedStationRequests = storedStationRequests diff stationItems
          stationItems = Nil
        }
      }
      
      val observationValuesCollection = createSensorObservationValuesCollection(station, sensor, phenomenon)
      
      if (stationItems.isEmpty) {
        // make a request with the given params
        // request the result for each station using its foreign_tag, description (which is the organization id)        
        var stItem: Option[StoretResponse] = tryGetResultsForStation(station.databaseStation.foreign_tag, station.databaseStation.description, startDate)
        stItem match {
          case Some(stItem) => {
            // add it in to singleton list
            storedStationRequests = stItem :: storedStationRequests
            // set it to list
            stationItems = List(stItem)
          }
          case None => {
              stationItems = Nil
          }
        }
      }
      
      // do we not have any phenomenon at this point?
      if (stationItems.isEmpty) {
        return Nil
      }
      
     // get list of observed values matching the phenomenon name, then iterate and add the values and dates for the observation (phenomenon)
      for (observationValue <- observationValuesCollection) {
        try {
          logger.info("looking for obsValue: " + phenomenon.databasePhenomenon.name)
          val observationList = stationItems.flatMap(itm => matchObsTags(phenomenon.databasePhenomenon.name, itm.obsList))
          logger.info("Have " + observationList.size + " observations for phenomenon")
          for (obs <- observationList; if !observationValue.containsDate(obs._1)) {
            observationValue.addValue(obs._2, obs._1)
          }
        } catch {
          case ex: Exception => { logger.info(ex.toString + "\n\t" + ex.getStackTraceString) }
        }
      }
      
      observationValuesCollection.filter(_.getValues.size > 0)
    }
    
  /**
   * attempts to match the names for phenomenon in the database to those provided by storet
   */
    private def matchObsTags(phenomName: String, observations: List[(String, List[(Calendar, Double)])]) : List[(Calendar, Double)] = {
      // need to compare the string value of observations to the phenomenon tag, unfortunately a direct comparison will not work here
      var retval: List[(Calendar, Double)] = Nil
      val lphenomName = phenomName.toLowerCase
      var primaryTagToSearch: String = "nothing"
      var secondaryTagToSearch: String = "nothing"
      if (lphenomName.contains("temperature")) {
        if (lphenomName.contains("water")) {
          secondaryTagToSearch = "water"
        }
        primaryTagToSearch = "temperature"
      } else if (lphenomName contains "acidity") {
        primaryTagToSearch = "ph"
      } else if (lphenomName.contains("_")) {
        primaryTagToSearch = lphenomName.split("_").head
        secondaryTagToSearch = (lphenomName.split("_").toList diff primaryTagToSearch).head
      } else if (lphenomName.contains("\\s+")) {
        primaryTagToSearch = lphenomName.split("\\s+").head
        secondaryTagToSearch = (lphenomName.split("\\s+").toList diff primaryTagToSearch).head
      } else {
        primaryTagToSearch = lphenomName
      }
      
      if (secondaryTagToSearch != "nothing") {
        observations.filter(p => p._1.toLowerCase.contains(primaryTagToSearch) && p._1.toLowerCase.contains(secondaryTagToSearch)).map(p => logger.info(p._1))
        retval = observations.filter(p => p._1.toLowerCase.contains(primaryTagToSearch) && p._1.toLowerCase.contains(secondaryTagToSearch)).flatMap(p => p._2)
      } else {
        retval = observations.filter(p => p._1.toLowerCase.contains(primaryTagToSearch)).flatMap(p => p._2)
      }
      
      if (retval.size > 0) {
        logger.info("returning obsrevations for " + lphenomName + ": ")
        retval.map(p => logger.info(p._1.getTime.toString + " - " + p._2.toString))
      }
      
      return retval
    }
    
    private def tryGetResultsForStation(stationId: String, orgId: String, startDate: Calendar) : Option[StoretResponse] = {
      // get date as a string for the request
      // use the startDate as min activity date (incrementing day of month by 1 to make sure we don't get repeat results)
      val startDateString = (startDate.get(Calendar.MONTH)+1) + "/" + (startDate.get(Calendar.DAY_OF_MONTH)+1) + "/" + startDate.get(Calendar.YEAR)
      val xmlRequest = <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:srs="http://storetresultservice.storet.epa.gov/">
                <soap:Body>
                  <srs:getResults>
                    <OrganizationId>{orgId}</OrganizationId>
                    <MonitoringLocationId>{stationId}</MonitoringLocationId>
                    <MinimumActivityStartDate>{startDateString}</MinimumActivityStartDate>
                    <CharacteristicName/><MonitoringLocationType/><MaximumActivityStartDate/><MinimumLatitude/><MaximumLatitude/><MinimumLongitude/><MaximumLongitude/><CharacteristicType/><ResultType/>
                  </srs:getResults>
                </soap:Body>
              </soap:Envelope>
      val response = httpSender.sendPostMessage(resultURL, xmlRequest.toString)
      if (response != null) {
        val responseFix = response.toString.trim.replaceAll("""(&lt;)""", """<""").replaceAll("""(&gt;)""", """>""").replaceAll("""<\?xml version=[\"]1.0[\"] encoding=[\"]UTF-8[\"]\?>""", "").replaceAll("\n", "")
        val xml = loadXMLFromString(responseFix)
        xml match {
          case Some(xml) => {
            // create a response object from the xml
            createObjFromResponse(xml, stationId, startDate)
          } case None => {
            None
          }
        }
      }
      else
        None
    }
    
    private def createObjFromResponse(xml: scala.xml.Elem, stationId: String, requestDate: Calendar) : Option[StoretResponse] = {
      var valuesList: List[(String, List[(Calendar,Double)])] = Nil
      // iterate through each activity and add the phenomenon name and its associated measured value for the date
      for (activity <- xml \\ "Activity") {
        val rawDate = (activity \\ "ActivityStartDate").text.trim
        val rawTime = (activity \\ "ActivityStartTime").text.trim
        val calendarDate = parseDateString(rawDate + " " + rawTime)
        // add a list of observations for each activity with its date
        val obsList = for (result <- activity \\ "Result") yield {
          val phenName = (result \\ "CharacteristicName").text.trim
          try {
            val measuredValue = (result \\ "ResultMeasureValue").text.trim.toDouble
            val obsTuple = (calendarDate, measuredValue)
            val storedPhen = valuesList.filter(p => p._1.equalsIgnoreCase(phenName))
            if (storedPhen.nonEmpty) {
              // disect the valuesList entry to expand its observation tuple list
              var (name, pObsList) = storedPhen.head
              // remove the item from list
              valuesList = valuesList diff List((name, pObsList))
              pObsList = obsTuple :: pObsList
              // add it back in
              valuesList = (name, pObsList) :: valuesList
            } else {
              // add it in
              valuesList = (phenName, List(obsTuple)) :: valuesList
            }
          } catch {
            case ex: Exception => {
                // don't add it to the list
                logger.error("Unable to add a measured value for " + phenName + "\n\t" + ex.toString)
            }
          }
        }
      }
      
      if (valuesList.nonEmpty) {
        Some(new StoretResponse(stationId, requestDate, valuesList))
      } else {
        None
      }
    }
    
    private def getResults(stationId : String, orgId : String, phenomenonName : String, startDate : String) : Option[scala.xml.Elem] = {
      val xmlRequest = <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:srs="http://storetresultservice.storet.epa.gov/">
                <soap:Body>
                  <srs:getResults>
                    <OrganizationId>{orgId}</OrganizationId>
                    <MonitoringLocationId>{stationId}</MonitoringLocationId>
                    <CharacteristicName>{phenomenonName}</CharacteristicName>
                    <MinimumActivityStartDate>{startDate}</MinimumActivityStartDate>
                    <MonitoringLocationType/><MaximumActivityStartDate/><MinimumLatitude/><MaximumLatitude/><MinimumLongitude/><MaximumLongitude/><CharacteristicType/><ResultType/>
                  </srs:getResults>
                </soap:Body>
              </soap:Envelope>
      val response = httpSender.sendPostMessage(resultURL, xmlRequest.toString)
      if (response != null) {
        val responseFix = response.toString.replaceAll("""(&lt;)""", """<""").replaceAll("""(&gt;)""", """>""").replaceAll("""<\?xml version=[\"]1.0[\"] encoding=[\"]UTF-8[\"]\?>""", "").replaceAll("\n", "")
        loadXMLFromString(responseFix)
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
}
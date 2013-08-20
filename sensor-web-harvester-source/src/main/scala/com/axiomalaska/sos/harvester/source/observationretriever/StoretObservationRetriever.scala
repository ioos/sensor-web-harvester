/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.harvester.source.observationretriever

import java.util.Calendar

import scala.Array.canBuildFrom

import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.ObservationValues
import com.axiomalaska.sos.tools.HttpSender

object StoretObservationRetriever {
  private var storedStationResponse: (String,List[String]) = ("",Nil)
}

class StoretObservationRetriever(private val stationQuery:StationQuery)
	extends ObservationValuesCollectionRetriever {
import StoretObservationRetriever._
    private val LOGGER = Logger.getLogger(getClass())
          
    private val dateParser = DateTimeFormat.forPattern("MM/dd/yyyyHH:mm:ssz")
    private val resultURL = "http://www.waterqualitydata.us/Result/search?countrycode=US&mimeType=csv"
    
  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
    phenomenon: LocalPhenomenon, startDate: DateTime):List[ObservationValues] = {
 
    LOGGER.info("STORET: Collecting for station - " + 
        station.databaseStation.foreign_tag + " - observation - " + 
        phenomenon.databasePhenomenon.tag)
      
    // request info for the station
    getStationResponse(station, startDate)
    
    if (storedStationResponse._2.isEmpty) {
      LOGGER info "No result for " + station.databaseStation.foreign_tag
      return Nil
    }
      
    val observationValuesCollection = 
      createSensorObservationValuesCollection(station, sensor, phenomenon)
      
    // iterate through observation values, getting values for each phenomenon
    for (observationValue <- observationValuesCollection) {
      val timeandvalues = getValuesDateDepths(observationValue.phenomenon.getName)
      for {
        addto <- timeandvalues
        if (observationValue.observedProperty.depth == addto._3 && 
            !observationValue.containsDate(addto._1))
      } {
        observationValue.addValue(addto._2, addto._1)
      }
    }
    // remove any empty values from the collection
    observationValuesCollection.filter(_.getValues.size > 0)
  }
    
  private def getStationResponse(station: LocalStation, startDate: DateTime) = {
    // check to see if it is in our stored station retriever
    if (!storedStationResponse._1.equals(station.getId)) {
      LOGGER info "Sending request for station " + station.databaseStation.foreign_tag
      // make request
      val siteid = station.databaseStation.foreign_tag
      val org = siteid.split("-").head
      // get date for latest mm-dd-yyyy (month is 0 index, so increment by 1, day is incremented to prevent repeat data
      val date = startDate.getMonthOfYear() + "-" + (startDate.getDayOfMonth() + 1) + "-" + startDate.getYear()
      // add in above to formulate request
      val request = resultURL + "&organization=" + org + "&siteid=" + siteid + "&startDateLo=" + date
      try {
        val response = HttpSender.sendGetMessage(request)
        // TEST File below
  //      val response = scala.io.Source.fromFile("../Result.csv", "UTF-8").mkString
        if (response != null) {
          // add the filtered response to our stored request
          val splitResponse = response.mkString.split('\n')
          val removeFirstRow = splitResponse.toList.filter(
              s => !s.contains("OrganizationIdentifier")).filter(
                  p => p.contains(station.databaseStation.foreign_tag))
          storedStationResponse = (station.getId,removeFirstRow)
        } else {
          LOGGER warn "Response in getting station info was null"
          storedStationResponse = ("",Nil)
        }
      }
      catch {
        case ex: Exception => {
            LOGGER error ex.toString
            storedStationResponse = ("",Nil)
        }
      }
    }
  }
  
  private def filterCSV(csv: List[String]) : List[String] = {
    var inQuote: Boolean = false
    csv map { l => {
      val newString = for (ch <- l) yield ch match {
        case '"' if (!inQuote) => { inQuote = true; '\0' }
        case '\t' if (inQuote) => '\0'
        case '"' if (inQuote) => { inQuote = false; '\0' }
        case default => default
      }
      newString filter ( _ != '\0' )
    } }
  }
  
  private def getValuesDateDepths(phenom: String) : List[(DateTime, Double, Double)] = {
    // get a grouping of the characteristic names (index 31)
    val indexedLines = storedStationResponse._2.filter(!_.contains("Non-detect")).map(_.split("\t")).map(_.zipWithIndex)
    // uggh, so i need like 6 indices from the info (3 of which are combined into one);
    // index 1: date-time, index 2: name, index 3: value, index 4: depth
    val valMap = indexedLines.map(s => s.foldLeft("", "", "", "")((storedTuple, nextIndex) => nextIndex._2 match {
      case 6 | 7 => (storedTuple._1 + nextIndex._1, storedTuple._2, storedTuple._3, storedTuple._4)
      case 8 => if (nextIndex._1 == null || nextIndex._1.equals("")) { 
        (storedTuple._1 + "UTC", storedTuple._2, storedTuple._3, storedTuple._4) 
        } else { 
          (storedTuple._1 + nextIndex._1, storedTuple._2, storedTuple._3, storedTuple._4) 
        }
      case 31 => (storedTuple._1, nextIndex._1, storedTuple._3, storedTuple._4)
      case 33 => (storedTuple._1, storedTuple._2, nextIndex._1, storedTuple._4)
      case 12 => (storedTuple._1, storedTuple._2, storedTuple._3, nextIndex._1)
      case _ => storedTuple
    }))
    // group-by on the name; first have to convert the name to how it would look on the server
    val grouped = valMap.groupBy( n => getDBNameForPhenomenon(n._2) )
    // ok now just match and return the dates/values
    // okay so step by step
    // 1. filter out names that do not match what we want
    // 2. flatmap over the results of 1. to get the list of the tuples (from above)
    // 3. map over each list of tuple to convert the datetime string into Calenar and parse each value, depth into double
    // 4. group by the datetime (Calendar) as we don't want to add in multiple values for each phenomenon/datetime pair
    // 5. map over the group in 4. to get a new tuple of explicit datetime/values and return it as a list
    // 6. filter out any null calendar objects
    grouped.filter(g => g._1.equalsIgnoreCase(phenom)).flatMap(_._2.map(tuple => {
        LOGGER.info("Processing:  " + tuple._1 + ", " + tuple._2 + ", " + tuple._3 + ", " + tuple._4)
        var datetime: DateTime = null
        var value: Double = 0.0
        var depth: Double = 0.0
        try {
          depth = java.lang.Double.parseDouble(tuple._4)
        } catch {
          case ex: Exception => {}
        }
        try {
          datetime = parseDateString(tuple._1)
          value = java.lang.Double.parseDouble(tuple._3)
        } catch {
          case ex: Exception => {
              LOGGER error ex.toString
              value = Double.NaN
          }
        }
        (datetime,value,depth)
      })).toList
  }
  
  /**
   * copied just about wholesale from the updater; exception: removed reference to units string
   */
  private def getDBNameForPhenomenon(name: String) : String = {
    val lname = name.toLowerCase
    if (lname.equals("ammonium") || lname.equals("ammonium as n")) {
      Phenomena.instance.AMMONIUM.getName
    } else if (lname.equals("chlorophyll") || lname.equals("chlorophyll a free of pheophytin")) {
      Phenomena.instance.CHLOROPHYLL.getName
    } else if (lname equals "chlorophyll_flourescence") {
      Phenomena.instance.CHLOROPHYLL_FLOURESCENCE.getName
    } else if (lname.equals("nitrite+nitrate") || lname.equals("inorganic nitrogen (nitrate and nitrite) as n")) {
      Phenomena.instance.NITRITE_PLUS_NITRATE.getName
    } else if (lname equals "nitrite") {
      Phenomena.instance.NITRITE.getName
    } else if (lname equals "nitrate") {
      Phenomena.instance.NITRATE.getName
    } else if (lname.equals("temperature water")) {
      Phenomena.instance.SEA_WATER_TEMPERATURE.getName
    } else if (lname equals "speed water") {     // not sure if this is actually a variable name in storet
      Phenomena.instance.SEA_WATER_SPEED.getName
    } else if (lname.equals("phosphorus as p")) {
      Phenomena.instance.PHOSPHORUS.getName
    } else if (lname.equals("wind direction") || lname.equals("wind direction (direction from expressed 0-360 deg)")) {  // not sure if this is actually a variable name in storet
      Phenomena.instance.WIND_FROM_DIRECTION.getName
    } else if (lname equals "wind gust") {       // not sure if this is actually a variable name in storet
      Phenomena.instance.WIND_SPEED_OF_GUST.getName
    } else if (lname.equals("temperature air")) {
      Phenomena.instance.AIR_TEMPERATURE.getName
    } else if (lname equals "dew") {
      Phenomena.instance.DEW_POINT_TEMPERATURE.getName
    } else if (lname equals "ph") {
      Phenomena.instance.SEA_WATER_PH_REPORTED_ON_TOTAL_SCALE.getName
    } else if (lname.equals("alkalinity total (total hydroxide+carbonate+bicarbonate)") || lname.equals("alkalinity total as caco3")) {
      Phenomena.instance.ALKALINITY.getName
    } else if (lname.equals("wave height")) {
      Phenomena.instance.SEA_SURFACE_WIND_WAVE_SIGNIFICANT_HEIGHT.getName
    } else if (lname.equals("water level reference point elevation") || lname.equals("water level in relation to reference point")) {
      Phenomena.instance.WATER_SURFACE_HEIGHT_ABOVE_REFERENCE_DATUM.getName
    } else if (lname.equals("specific conductance")) {
      Phenomena.instance.SEA_WATER_ELECTRICAL_CONDUCTIVITY.getName
    } else {
      // create a homeless parameter
      Phenomena.instance.createHomelessParameter(nameToTag(lname), "").getName
    }
  }
  
  private def nameToTag(name: String) : String = {
    name.trim.toLowerCase.replaceAll("""[\s-]+""", "_").replaceAll("""[\W]+""", "")
  }
    
  /**
   * attempts to match the names for phenomenon in the database to those provided by storet
   */
    private def matchObsTags(phenomName: String, observations: List[(String, List[(Calendar, Double)])]) : List[(Calendar, Double)] = {
      // need to compare the string value of observations to the phenomenon tag, unfortunately a direct comparison will not work here
      var retval: List[(Calendar, Double)] = Nil
      
      return retval
    }
    
  private def createSensorObservationValuesCollection(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    for (observedProperty <- observedProperties) yield {
      new ObservationValues(observedProperty, sensor, phenomenon, observedProperty.foreign_units)
    }
  }
            
  private def parseDateString(rawString : String) = 
    dateParser.parseDateTime(rawString)
}

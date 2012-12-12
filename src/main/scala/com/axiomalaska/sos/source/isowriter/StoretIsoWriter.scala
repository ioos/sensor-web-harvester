/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source.isowriter

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.tools.HttpSender
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.log4j.Logger

class StoretIsoWriter(private val stationQuery:StationQuery, 
    private val isoTemplateLocation: String,
    private val isoWriteDirectory: String,
    private val logger: Logger = Logger.getRootLogger()) extends ISOWriterImpl(stationQuery, isoTemplateLocation, isoWriteDirectory, logger) {

    private val httpSender = new HttpSender()
    private val dateParser = new SimpleDateFormat("yyyy-MM-ddHH:mm:ssz")
    
    // url for querying storet stations, need to add 'organization' and 'siteid' for a successful query
    private val surl = "http://www.waterqualitydata.us/Station/search?countrycode=US&command.avoid=NWIS&mimeType=csv"
    private val rurl = "http://www.waterqualitydata.us/Result/search?countrycode=US&command.avoid=NWIS&mimeType=csv"
    
    var stationResponse: List[String] = null
    var resultResponse: List[String] = null
    
    override def initialSetup(station: LocalStation) : Boolean = {
      // get org id and site id
      val site = station.databaseStation.foreign_tag
      val org = site.split("-").head
      // request the station info
      try {
        val sResponse = httpSender.sendGetMessage(surl + "&organization=" + org + "&siteid=" + site)
        if (sResponse != null) {
          val splitResponse = sResponse.toString split '\n'
          val removeFirstRow = splitResponse.toList.filter(s => !s.contains("OrganizationIdentifier"))
          stationResponse = filterCSV(removeFirstRow)
        } else {
          logger error "Could not get station information for " + site
          return false
        }

        val rResponse = httpSender.sendGetMessage(rurl + "&organization=" + org + "&siteid=" + site)
        if (rResponse != null) {
          val spltResponse = rResponse.toString split '\n'
          val removeFirstRow = spltResponse.toList.filter(s => !s.contains("OrganizationIdentifier"))
          resultResponse = filterCSV(removeFirstRow)
          logger info "result line count: " + resultResponse.map(s => 1).foldLeft(0)(_+_)
        } else {
          logger error "Could not get station results for " + site
          return false
        }
      } catch {
        case ex: Exception => {
            logger error ex.toString
            return false
        }
      }
      
      true
    }
    
    override def getStationName(station: LocalStation) : String = {
      // get the 3rd index of the station response when split on ','
      val splitLines = stationResponse map { _.split(",") } 
      // okay so below allows us to search by index looking for the earliest instance of index 3
      (splitLines map { _.zipWithIndex } flatten).foldRight(("", 0))((a,b) => { b._2 match { case 3 => (b._1, b._2); case _ => (a._1, a._2) } } )._1.replaceAll("\"","")
    }
    
    override def getStationID(station: LocalStation) : String = {
      // get the 2nd index of the station response when split on ','
      val splitLines = stationResponse map { _.split(",") }
      // okay so below allows us to search by index looking for the earliest instance of index 2
      (splitLines map { _.zipWithIndex } flatten).foldRight(("", 0))((a,b) => { b._2 match { case 2 => (b._1, b._2); case _ => (a._1, a._2) } } )._1.replaceAll("\"","")
    }
    
    override def getStationAbstract(station: LocalStation) : String = {
      // get the 1st and 3rd indices, combine them for our abstract
      val splitLines = stationResponse map { _.split(",") }
      val orgName = (splitLines map { _.zipWithIndex } flatten).foldRight(("",0))((a,b) => { b._2 match { case 1 => (b._1, b._2); case _ => (a._1, a._2) } } )._1.replaceAll("\"","")
      val locName = (splitLines map { _.zipWithIndex } flatten).foldRight(("",0))((a,b) => { b._2 match { case 3 => (b._1, b._2); case _ => (a._1, a._2) } } )._1.replaceAll("\"","")
      orgName + " - " + locName
    }
    
    override def getStationTemporalExtents(station: LocalStation) : (Calendar,Calendar) = {
      try {
        var startDate: Calendar = Calendar.getInstance
        var endDate: Calendar = Calendar.getInstance
        endDate.setTimeInMillis(0)
        val splitLines = resultResponse map { _.split(",") }
        for (line <- splitLines) {
          val other = (line zipWithIndex) map (i => (i._1,i._2,""))
          // below creates a tuple seq of (String, int, String), folds them adding the values of index 6,7,8 together and returns this
          // the first string is the value of the index
          // the int is the index
          // the third value is a running string that is added to from the value of the index
          val timestring = ((line zipWithIndex) map (i => (i._1,i._2,""))).foldRight("",0,"")((a,b) => { a._2 match { case 6|7 => ("",0,a._1 + b._3); case 8 => if (a._1.length < 1) ("",0,"GMT") else ("",0,a._1); case _ => ("",0,b._3) } } )._3
          // parse time string, compare to our start & end times
          val date = Calendar.getInstance
          date.setTimeInMillis(dateParser.parse(timestring).getTime)
          if (date.after(endDate))
            endDate = date
          if (date.before(startDate))
            startDate = date
        }
        (startDate,endDate)
      } catch {
        case ex: Exception => {
            logger error ex.toString
            (null, null)
        }
      }
    }
    
    override def getSensorTagsAndNames(station: LocalStation) : List[(String,String)] = {
      // go through the results and get the names of all lines w/o 'Non-detect'
      val splitLines = resultResponse filter { !_.contains("Non-detect") } map { _.split(",") }
      // 'charactername' is at index 31
      (splitLines map { _.zipWithIndex } flatten) map { s=> s._2 match { case 31 => ( s._1, s._1 ); case _ => ( "", "" ) } } filter { _._1.length > 0 } groupBy { _._1 } map { _._2.head } toList
    }
    
    private def filterCSV(csv: List[String]) : List[String] = {
      var inQuote: Boolean = false
      csv map { l => {
        val newString = for (ch <- l) yield ch match {
          case '"' if (!inQuote) => { inQuote = true; '\0' }
          case ',' if (inQuote) => '\0'
          case '"' if (inQuote) => { inQuote = false; '\0' }
          case default => default
        }
        newString filter ( _ != '\0' )
      } }
    }
}

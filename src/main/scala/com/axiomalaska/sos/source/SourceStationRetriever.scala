package com.axiomalaska.sos.source

import com.axiomalaska.sos.source.data.LocalStation
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.axiomalaska.sos.StationRetriever
import com.axiomalaska.sos.data.SosNetwork
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.source.data.LocalSource

/**
 * A StationRetriever for all of the sources in the package. All one needs to 
 * do is pass in the source id. From this source id all SosStations can be created. 
 */
class SourceStationRetriever(
    private val stationQuery:StationQuery, 
    val sourceId: Int, rootNetwork:SosNetwork,
    private val logger: Logger = Logger.getRootLogger()) extends StationRetriever {
    
    private var stationList: List[LocalStation] = Nil

  override def getStations(): java.util.List[SosStation] ={
//      val source = stationQuery.getSource(sourceId)
//      
//      val sosSource = new LocalSource(source)
//      
//      val databaseStations = stationQuery.getActiveStations(source)
//      
//      val sosStations = databaseStations.map(station => 
//        new LocalStation(sosSource, station, stationQuery))
//        
//      return sosStations
//      
    if (stationList.isEmpty) {
      populateStationList
    }
    
    return stationList
  }
  
  def getLocalStations : List[LocalStation] = {
    if (stationList.isEmpty)
      populateStationList
    
    return stationList
  }
  
  def populateStationList() : List[LocalStation] = {
      val source = stationQuery.getSource(sourceId)
      
      val sosSource = new LocalSource(source)
      
      val databaseStations = stationQuery.getActiveStations(source)
      
      val sosStations = databaseStations.map(station => 
        new LocalStation(sosSource, station, stationQuery, rootNetwork))
        
      return sosStations
  }
  
  
}
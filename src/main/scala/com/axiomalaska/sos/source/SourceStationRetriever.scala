package com.axiomalaska.sos.source

import com.axiomalaska.sos.StationRetriever
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.source.data.LocalStation
import scala.collection.JavaConversions._

class SourceStationRetriever(
    private val stationQuery:StationQuery, 
    val sourceId: Int) extends StationRetriever {

  override def getStations(): java.util.List[SosStation] ={
      val source = stationQuery.getSource(sourceId)
      
      for {
        station <- stationQuery.getStations(source)
      } yield {
    	  new LocalStation(station, stationQuery)
      }
  }
}
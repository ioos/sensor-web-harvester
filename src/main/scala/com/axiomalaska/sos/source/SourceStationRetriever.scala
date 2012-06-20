package com.axiomalaska.sos.source

import com.axiomalaska.sos.StationRetriever
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.source.data.LocalStation
import scala.collection.JavaConversions._

/**
 * A StationRetriever for all of the sources in the package. All one needs to 
 * do is pass in the source id. From this source id all SosStations can be created. 
 */
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
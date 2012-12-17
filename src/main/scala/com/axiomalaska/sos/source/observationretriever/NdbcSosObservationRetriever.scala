package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.ObservationRetriever
import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.data.ObservationCollection
import scala.collection.mutable
import scala.collection.JavaConversions._
import org.apache.log4j.Logger

class NdbcSosObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger()) 
	extends SosObservationRetriever(stationQuery, logger) {
  
  protected val serviceUrl = "http://sdf.ndbc.noaa.gov/sos/server.php"
  
}
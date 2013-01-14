package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.source.StationQuery
import scala.collection.mutable
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.axiomalaska.sos.source.SourceUrls

class NdbcSosObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger()) 
	extends SosObservationRetriever(stationQuery, logger) {
  
  protected val serviceUrl = SourceUrls.NDBC_SOS
  
}
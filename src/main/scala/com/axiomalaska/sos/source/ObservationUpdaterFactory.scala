package com.axiomalaska.sos.source

import org.apache.log4j.Logger
import com.axiomalaska.sos.ObservationUpdater
import com.axiomalaska.sos.source.observationretriever.RawsObservationRetriever
import com.axiomalaska.sos.source.observationretriever.NoaaNosCoOpsObservationRetriever
import com.axiomalaska.sos.source.observationretriever.HadsObservationRetriever
import com.axiomalaska.sos.source.observationretriever.NdbcObservationRetriever
import com.axiomalaska.sos.source.observationretriever.SnoTelObservationRetriever
import com.axiomalaska.sos.source.observationretriever.UsgsWaterObservationRetriever
import com.axiomalaska.sos.source.observationretriever.NoaaWeatherObservationRetriever
import com.axiomalaska.sos.source.data.SourceId

class ObservationUpdaterFactory {

  def buildRawsObservationUpdater(sosUrl: String, 
      logger: Logger, stationQuery:StationQuery): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.RAWS)
    val observationRetriever = new RawsObservationRetriever(stationQuery)

    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, observationRetriever)
    
    return observationUpdater
  }
  
  def buildNoaaNosCoOpsObservationUpdater(sosUrl: String, 
      logger: Logger, stationQuery:StationQuery): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NOAA_NOS_CO_OPS)
    val observationRetriever = new NoaaNosCoOpsObservationRetriever(stationQuery)

    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, observationRetriever)
    
    return observationUpdater
  }
  
  def buildHadsObservationUpdater(sosUrl: String, 
      logger: Logger, stationQuery:StationQuery): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.HADS)
    val observationRetriever = new HadsObservationRetriever(stationQuery)

    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, observationRetriever)
    
    return observationUpdater
  }
  
  def buildNdbcObservationUpdater(sosUrl: String, 
      logger: Logger, stationQuery:StationQuery): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NDBC)
    val observationRetriever = new NdbcObservationRetriever(stationQuery)

    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, observationRetriever)
    
    return observationUpdater
  }
  
  def buildSnotelObservationUpdater(sosUrl: String, 
      logger: Logger, stationQuery:StationQuery): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.SNOTEL)
    val observationRetriever = new SnoTelObservationRetriever(stationQuery)

    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, observationRetriever)
    
    return observationUpdater
  }
  
  def buildUsgsWaterObservationUpdater(sosUrl: String, 
      logger: Logger, stationQuery:StationQuery): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.USGSWATER)
    val observationRetriever = new UsgsWaterObservationRetriever(stationQuery)

    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, observationRetriever)
    
    return observationUpdater
  }
  
  def buildNoaaWeatherObservationUpdater(sosUrl: String, 
      logger: Logger, stationQuery:StationQuery): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NOAA_WEATHER)
    val observationRetriever = new NoaaWeatherObservationRetriever(stationQuery)

    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, observationRetriever)
    
    return observationUpdater
  }
}
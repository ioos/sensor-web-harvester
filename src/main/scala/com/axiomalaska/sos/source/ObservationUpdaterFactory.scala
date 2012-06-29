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

  /**
   * Build all the Source ObservationUpdaters
   */
  def buildAllSourceObservationUpdaters(sosUrl: String,
    stationQuery: StationQuery,
    logger: Logger = Logger.getRootLogger()): List[ObservationUpdater] =
    List(buildRawsObservationUpdater(sosUrl, stationQuery, logger),
      buildNoaaNosCoOpsObservationUpdater(sosUrl, stationQuery, logger),
      buildHadsObservationUpdater(sosUrl, stationQuery, logger),
      buildNdbcObservationUpdater(sosUrl, stationQuery, logger),
      buildSnotelObservationUpdater(sosUrl, stationQuery, logger),
      buildUsgsWaterObservationUpdater(sosUrl, stationQuery, logger),
      buildNoaaWeatherObservationUpdater(sosUrl, stationQuery, logger))
  
  /**
   * Build a RAWS ObservationUpdater
   */
  def buildRawsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger= Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.RAWS)
    val observationRetriever = new RawsObservationRetriever(stationQuery)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NOAA NOS CO-OPS ObservationUpdater
   */
  def buildNoaaNosCoOpsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger= Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NOAA_NOS_CO_OPS)
    val observationRetriever = new NoaaNosCoOpsObservationRetriever(stationQuery)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a HADS ObservationUpdater
   */
  def buildHadsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger= Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.HADS)
    val observationRetriever = new HadsObservationRetriever(stationQuery)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NDBC ObservationUpdater
   */
  def buildNdbcObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NDBC)
    val observationRetriever = new NdbcObservationRetriever(stationQuery)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a SnoTel ObservationUpdater
   */
  def buildSnotelObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.SNOTEL)
    val observationRetriever = new SnoTelObservationRetriever(stationQuery)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a USGS Water ObservationUpdater
   */
  def buildUsgsWaterObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.USGSWATER)
    val observationRetriever = new UsgsWaterObservationRetriever(stationQuery)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NOAA Weather ObservationUpdater
   */
  def buildNoaaWeatherObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NOAA_WEATHER)
    val observationRetriever = new NoaaWeatherObservationRetriever(stationQuery)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
}
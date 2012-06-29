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

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.RAWS, logger)
    val observationRetriever = new RawsObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NOAA NOS CO-OPS ObservationUpdater
   */
  def buildNoaaNosCoOpsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger= Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NOAA_NOS_CO_OPS, logger)
    val observationRetriever = new NoaaNosCoOpsObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a HADS ObservationUpdater
   */
  def buildHadsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger= Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.HADS, logger)
    val observationRetriever = new HadsObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NDBC ObservationUpdater
   */
  def buildNdbcObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NDBC, logger)
    val observationRetriever = new NdbcObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a SnoTel ObservationUpdater
   */
  def buildSnotelObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.SNOTEL, logger)
    val observationRetriever = new SnoTelObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a USGS Water ObservationUpdater
   */
  def buildUsgsWaterObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.USGSWATER, logger)
    val observationRetriever = new UsgsWaterObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NOAA Weather ObservationUpdater
   */
  def buildNoaaWeatherObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NOAA_WEATHER, logger)
    val observationRetriever = new NoaaWeatherObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, retrieverAdapter)
    
    return observationUpdater
  }
}
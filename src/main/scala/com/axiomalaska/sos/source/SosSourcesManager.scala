package com.axiomalaska.sos.source

import com.axiomalaska.sos.source.stationupdater.AggregateStationUpdater
import org.apache.log4j.Logger
import scala.util.Random
import java.util.Calendar
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.sos.data.PublisherInfo

/**
 * This class manages updating the SOS with all the stations from the metadata database
 * 
 * @param databaseUrl - a database URL of the form <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
 * 	     example jdbc:postgresql://localhost:5432/sensor
 * @param databaseUser - the username for the database
 * @param databasePassword - the password for the database
 * @param sosUrl - The URL of the SOS example http://localhost:8080/sos/sos
 */
class SosSourcesManager(
    private val databaseUrl:String, 
	private val databaseUser:String, 
	private val databasePassword:String, 
	private val sosUrl:String, 
	private val publisherInfo:PublisherInfo, 
        private val sources: String,
	private val logger: Logger = Logger.getRootLogger()) {

  private val random = new Random(Calendar.getInstance.getTime.getTime)
  
  def updateSos() {
    
    val factory = new ObservationUpdaterFactory()
    val queryBuilder = new StationQueryBuilder(
      databaseUrl, databaseUser, databasePassword)

    queryBuilder.withStationQuery(stationQuery => {
      val observationUpdaters = factory.buildAllSourceObservationUpdaters(
        sosUrl, stationQuery, publisherInfo, sources.toLowerCase, logger)

      // load phenomenon
      val phenomena = stationQuery.getPhenomena
      for (phenom <- phenomena) {
        Phenomena.instance.createHomelessParameter(phenom.tag.substring(phenom.tag.lastIndexOf("/") + 1), phenom.units)
      }
        
      for (observationUpdater <- random.shuffle(observationUpdaters)) {
        logger.info("Running updater: " + observationUpdater.toString)
        observationUpdater.update()
      }
    })
  }
}
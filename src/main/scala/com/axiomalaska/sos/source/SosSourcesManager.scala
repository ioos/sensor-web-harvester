package com.axiomalaska.sos.source

import com.axiomalaska.sos.source.stationupdater.AggregateStationUpdater
import org.apache.log4j.Logger
import scala.util.Random
import java.util.Calendar
import com.axiomalaska.sos.data.PublisherInfo
import com.axiomalaska.sos.data.SosNetwork

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
	private val rootNetwork:SosNetwork,
	private val logger: Logger = Logger.getRootLogger()) {

  private val random = new Random(Calendar.getInstance.getTime.getTime)

  def updateSos() {
    val factory = new ObservationUpdaterFactory()
    val queryBuilder = new StationQueryBuilder(
      databaseUrl, databaseUser, databasePassword)

    queryBuilder.withStationQuery(stationQuery => {
      val observationUpdaters = factory.buildAllSourceObservationUpdaters(
        sosUrl, stationQuery, publisherInfo, rootNetwork, logger)

      for (observationUpdater <- random.shuffle(observationUpdaters)) {
        observationUpdater.update(rootNetwork)
      }
    })
  }
}
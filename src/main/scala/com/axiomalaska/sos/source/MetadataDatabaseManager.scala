package com.axiomalaska.sos.source

import com.axiomalaska.sos.source.stationupdater.AggregateStationUpdater

/**
 * This class manages updating the database with all the stations from all the 
 * sources for a selected region. 
 */
class MetadataDatabaseManager(    
    private val databaseUrl:String, 
	private val databaseUser:String, 
	private val databasePassword:String,
	private val boundingBox:BoundingBox) {

  def update(){
    val factory = new ObservationUpdaterFactory()
    val queryBuilder = new StationQueryBuilder(
      databaseUrl, databaseUser, databasePassword)

      
    queryBuilder.withStationQuery(stationQuery => {
      val stationUpdater = new AggregateStationUpdater(stationQuery, boundingBox)
      stationUpdater.update()
    })
  }
}
package com.axiomalaska.sos.harvester.app

import com.axiomalaska.sos.harvester.BoundingBox;
import com.axiomalaska.sos.harvester.StationQueryBuilder;

/**
 * This class manages updating the database with all the stations from all the 
 * sources for a selected region. 
 */
class MetadataDatabaseManager(    
    private val databaseUrl:String) {
  val queryBuilder = new StationQueryBuilder(databaseUrl)

  def init() {
    queryBuilder.withStationQuery(stationQuery => {
      stationQuery.init
    })
  }
  
  def update(boundingBox:BoundingBox, sources: String) {
    queryBuilder.withStationQuery(stationQuery => {
      val stationUpdater = new AggregateStationUpdater(stationQuery, 
          boundingBox, sources)
      stationUpdater.update()
    })
  }
}
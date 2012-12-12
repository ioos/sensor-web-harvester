/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source

import org.apache.log4j.Logger
import com.axiomalaska.sos.source.isowriter._

class ISOSourcesManager(private val isoTemplate: String,
                        private val isoDirectory: String,
                        private val sources: String,
                        private val databaseUrl: String,
                        private val databaseUsername: String,
                        private val databasePassword: String,
                        private val logger: Logger = Logger.getRootLogger()) {
  
  def writeISOs() {
    val queryBuilder = new StationQueryBuilder(databaseUrl, databaseUsername, databasePassword)
    queryBuilder.withStationQuery(stationQuery => {
        val isoWriters = new AggregateIsoWriter(stationQuery, isoTemplate, isoDirectory, sources, logger)
        isoWriters.writeISOs()
      })
  }
}

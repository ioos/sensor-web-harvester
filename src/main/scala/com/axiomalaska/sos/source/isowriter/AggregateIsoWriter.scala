/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source.isowriter

import com.axiomalaska.sos.data.SosNetwork
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.LocalSource
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.source.data.Source
import org.apache.log4j.Logger

class AggregateIsoWriter(private val stationQuery: StationQuery,
                         private val templateFile: String,
                         private val isoDirectory: String,
                         private val sources: String,
                         private val overwrite: Boolean = true,
                         private val rootNetwork: SosNetwork,
                         private val logger: Logger = Logger.getRootLogger()) {

  def writeISOs() {
    val writers = getSourceWriters()
    
    writers.foreach(
      w => {
        val src = w._1
        val wrt = w._2
        for (station <- stationQuery.getAllStations(src)) {
          wrt.writeISOFile(new LocalStation(new LocalSource(src), station, stationQuery, rootNetwork))
        }
      }
    )
  }
  
  private def getSourceWriters() : List[(Source,ISOWriter)] = {
    val sourceSplit = sources.toLowerCase split ";"
    val dbSources = stationQuery.getAllSource
    val writers = for (src <- sourceSplit) yield src match {
      case "ndbc" => {
          val thisSource = dbSources.filter( _.tag.equalsIgnoreCase("ndbc") ).head
          new Some((thisSource,new NdbcIsoWriter(stationQuery, templateFile, isoDirectory, overwrite, logger)))
      }
      case "storet" => {
          val thisSource = dbSources.filter( _.tag.equalsIgnoreCase("storet") ).head
          new Some((thisSource,new StoretIsoWriter(stationQuery, templateFile, isoDirectory, overwrite, logger)))
      }
      case _ => None
    }
    // return list that has all 'None' removed
    val retval = writers.filter( _.isDefined ).map( _.get )
    retval.toList
  }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.harvester.iso

import scala.Array.canBuildFrom

import org.apache.log4j.Logger

import com.axiomalaska.sos.data.PublisherInfo
import com.axiomalaska.sos.harvester.StationQuery
import com.axiomalaska.sos.harvester.data.LocalSource
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.Source

class AggregateIsoWriter(private val stationQuery: StationQuery,
                         private val templateFile: String,
                         private val isoDirectory: String,
                         private val sources: String,
                         private val overwrite: Boolean = true,
                         private val publisherInfo: PublisherInfo) {

  private val LOGGER = Logger.getLogger(getClass())
  
  def writeISOs() {
    val writers = getSourceWriters()
    
    writers.foreach(
      w => {
        val src = w._1
        val wrt = w._2
        LOGGER.info("src: " + src.toString + " wrt: " + wrt.toString)
        for (station <- stationQuery.getAllStations(src)) {
          wrt.writeISOFile(new LocalStation(new LocalSource(src), station, 
              stationQuery))
        }
      }
    )
  }
  
  private def getSourceWriters() : List[(Source,ISOWriter)] = {
    val sourceSplit = sources.toLowerCase split ";"
    val dbSources = stationQuery.getAllSource
    val writers = for (src <- sourceSplit) yield src match {
      case "ndbc" => {
          val thisSource = dbSources.filter( _.tag.equalsIgnoreCase("wmo") ).head
          new Some((thisSource,new NdbcSosIsoWriter(stationQuery, templateFile,
              isoDirectory, overwrite)))
      }
      case "storet" => {
          val thisSource = dbSources.filter( _.tag.equalsIgnoreCase("us.storet") ).head
          new Some((thisSource,new StoretIsoWriter(stationQuery, templateFile, 
              isoDirectory, overwrite)))
      }
      case "hads" => {
        val thisSource = dbSources.filter( _.tag.equalsIgnoreCase("gov.noaa.nws.hads")).head
        new Some((thisSource, new HadsIsoWriter(stationQuery, templateFile,
          isoDirectory, overwrite)))
      }
      case "noaanoscoops" => {
        val thisSource = dbSources.filter( _.tag.equalsIgnoreCase("NOAA.NOS.CO-OPS")).head
        new Some((thisSource, new NoaaNosCoOpsIsoWriter(stationQuery, templateFile,
          isoDirectory, overwrite)))
      }
      case _ => None
    }
    // return list that has all 'None' removed
    val retval = writers.filter( _.isDefined ).map( _.get )
    retval.toList
  }
}

package com.axiomalaska.sos.harvester.source.observationretriever

import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.ObservationValues
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import org.joda.time.DateTime

trait ObservationValuesCollectionRetriever {
  def getObservationValues(station: LocalStation,
    sensor: LocalSensor, phenomenon: LocalPhenomenon, startDate: DateTime): 
    List[ObservationValues]
}
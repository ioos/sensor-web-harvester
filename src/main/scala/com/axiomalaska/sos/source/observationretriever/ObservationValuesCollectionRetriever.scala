package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.source.data.LocalStation
import java.util.Calendar
import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.source.data.ObservationValues
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon

trait ObservationValuesCollectionRetriever {
  def getObservationValues(station: LocalStation,
    sensor: LocalSensor, phenomenon: LocalPhenomenon, startDate: Calendar): 
    List[ObservationValues]
}
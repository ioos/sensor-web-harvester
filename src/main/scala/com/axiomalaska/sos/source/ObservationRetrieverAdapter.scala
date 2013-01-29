package com.axiomalaska.sos.source

import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.data.SosStation
import java.util.Calendar
import com.axiomalaska.sos.data.ObservationCollection
import com.axiomalaska.sos.ObservationRetriever
import com.axiomalaska.sos.source.observationretriever.ObservationValuesCollectionRetriever
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.source.data.ObservationValues
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.axiomalaska.phenomena.Phenomenon

class ObservationRetrieverAdapter(retriever:ObservationValuesCollectionRetriever, 
    private val logger: Logger = Logger.getRootLogger()) 
	extends ObservationRetriever{

  // ---------------------------------------------------------------------------
  // ObservationRetriever Members
  // ---------------------------------------------------------------------------
  
  override def getObservationCollection( station:SosStation, 
	sensor:SosSensor, phenomenon:Phenomenon, startDate:Calendar): java.util.List[ObservationCollection] = {

    val observationValuesCollection = (station, sensor, phenomenon) match {
      case (localStation: LocalStation, localSensor: LocalSensor, localPhenomenon: LocalPhenomenon) => {
    	  retriever.getObservationValues(localStation, localSensor, localPhenomenon, startDate)
      }
      case _ => Nil
    }
    
    for(observationValuesCollection <- observationValuesCollection.filter(_.getDates.size > 0)) yield{
      createObservationCollection(station, observationValuesCollection)
    }
  }

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def createObservationCollection(station: SosStation,
    observationValues: ObservationValues): ObservationCollection = {
    
    val unitsConverter = UnitsConverter.getConverter(observationValues)
    val convertedObservationValues = unitsConverter.convert(observationValues)

    val observationCollection = new ObservationCollection()
    observationCollection.setObservationDates(convertedObservationValues.getDates)
    observationCollection.setObservationValues(convertedObservationValues.getValues)
    observationCollection.setPhenomenon(observationValues.phenomenon)
    observationCollection.setSensor(observationValues.sensor)
    observationCollection.setStation(station)
    
    if(observationValues.observedProperty.depth != 0.0){
      observationCollection.setDepth(observationValues.observedProperty.depth)
    }
    else{
      observationCollection.setDepth(null)
    }

    observationCollection
  }
}
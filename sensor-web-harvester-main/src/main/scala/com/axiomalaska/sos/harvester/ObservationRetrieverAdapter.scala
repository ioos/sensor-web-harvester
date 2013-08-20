package com.axiomalaska.sos.harvester

import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.data.ObservationCollection
import com.axiomalaska.sos.harvester.source.observationretriever.ObservationValuesCollectionRetriever
import com.axiomalaska.sos.ObservationRetriever
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.ObservationValues
import scala.collection.JavaConversions._
import com.axiomalaska.phenomena.Phenomenon
import org.joda.time.DateTime
import com.axiomalaska.ioos.sos.GeomHelper

class ObservationRetrieverAdapter(retriever:ObservationValuesCollectionRetriever) 
	extends ObservationRetriever{

  // ---------------------------------------------------------------------------
  // ObservationRetriever Members
  // ---------------------------------------------------------------------------
  
  override def getObservationCollection(
      sensor:SosSensor, phenomenon:Phenomenon, startDate:DateTime): java.util.List[ObservationCollection] = {

    val observationValuesCollection = (sensor.getStation(), sensor, phenomenon) match {
      case (localStation: LocalStation, localSensor: LocalSensor, localPhenomenon: LocalPhenomenon) => {
    	  retriever.getObservationValues(localStation, localSensor, localPhenomenon, startDate)
      }
      case _ => Nil
    }
    
    for(observationValuesCollection <- observationValuesCollection.filter(_.getDates.size > 0)) yield{
      createObservationCollection(sensor.getStation(), observationValuesCollection)
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
    
    for{(dateTime, value) <- convertedObservationValues.getDates.zip(convertedObservationValues.getValues)}{
      observationCollection.addObservationValue(dateTime, value)
    }

    observationCollection.setPhenomenon(observationValues.phenomenon)
    observationCollection.setSensor(observationValues.sensor)
    
    if(observationValues.observedProperty.depth != 0.0){
      observationCollection.setGeometry(
          GeomHelper.createLatLngPoint(station.getLocation().getY(), 
              station.getLocation().getX(), observationValues.observedProperty.depth * (-1)))
    }
    else{
      observationCollection.setGeometry(station.getLocation())
    }

    observationCollection
  }
}
package com.axiomalaska.sos.source.stationupdater

import org.apache.log4j.Logger

import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.source.observationretriever.SosRawDataRetriever
import com.axiomalaska.sos.source.data.Source
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty

import net.opengis.sos.x10.ObservationOfferingType
import net.opengis.sos.x10.CapabilitiesDocument
import net.opengis.sos.x10.GetCapabilitiesDocument
import net.opengis.om.x10.CompositeObservationDocument

import gov.noaa.ioos.x061.NamedQuantityType
import com.axiomalaska.sos.tools.HttpSender
import gov.noaa.ioos.x061.CompositePropertyType
import gov.noaa.ioos.x061.CompositeValueType
import gov.noaa.ioos.x061.ValueArrayType

abstract class SosStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val httpSender = new HttpSender()
  private val geoTools = new GeoTools()
  private val rawDataRetriever = new SosRawDataRetriever()
  protected val serviceUrl:String
  protected val source:Source

  // ---------------------------------------------------------------------------
  // StationUpdater Members
  // ---------------------------------------------------------------------------
  
  def update() {
    val sourceStationSensors = getSourceStations(source)

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }
  
  protected def sensorForeignNotUsed = List(
        "http://mmisw.org/ont/cf/parameter/harmonic_constituents", 
        "http://mmisw.org/ont/cf/parameter/datums", 
        "http://mmisw.org/ont/cf/parameter/currents")
        
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------
  
  private def getSourceStations(source: Source): List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    val observationOfferings = getObservationOfferingTypes()
    val size = observationOfferings.length - 1
    logger.info("Total number of stations not filtered: " + (size + 1))
    
    val stationSensorsCollection = for {
      (observationOfferingType, index) <- observationOfferings.zipWithIndex
      val station = createSouceStation(observationOfferingType, source)
      if (withInBoundingBox(station))
      val sourceObservedProperties = getSourceObservedProperties(
          observationOfferingType, station, source)
      val databaseObservedProperties = stationUpdater.updateObservedProperties(
          source, sourceObservedProperties)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if(sensors.nonEmpty)
    } yield {
      logger.debug("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }
    logger.info("finished with stations")
    
    return stationSensorsCollection
  }

  private def withInBoundingBox(station: DatabaseStation): Boolean = {
    val stationLocation = new Location(station.latitude, station.longitude)
    return geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }

  private def getObservationOfferingTypes(): List[ObservationOfferingType] = {
    getCapabilitiesDocument() match {
      case Some(capabilitiesDocument) => {
        capabilitiesDocument.getCapabilities().getContents().
        	getObservationOfferingList().getObservationOfferingArray().filter(
        	    observationOffering =>isStation(observationOffering)).toList
      }
      case None => {
        Nil
      }
    }
  }
  
  private def isStation(observationOffering:ObservationOfferingType):Boolean = {
    val procedures = observationOffering.getProcedureArray()
    if (procedures.nonEmpty) {
      val procedure = procedures(0)
      procedure.getHref().startsWith("urn:ioos:station:")
    } else {
      false
    }
  }
  
  protected def getCapabilitiesDocument(): Option[CapabilitiesDocument] = {
    val getCapabilitiesDocument = GetCapabilitiesDocument.Factory.newInstance

    val getCapabilities = getCapabilitiesDocument.addNewGetCapabilities
    getCapabilities.setService("SOS")

    val httpSender = new HttpSender()
    val results = httpSender.sendPostMessage(
        serviceUrl, getCapabilitiesDocument.xmlText());

    if(results != null){
    	Some(CapabilitiesDocument.Factory.parse(results))
    }
    else{
      None
    }
  }

  private def getSourceObservedProperties(observationOfferingType: ObservationOfferingType, 
      station: DatabaseStation, source: Source): List[ObservedProperty] = {
    val foreignObservatedProperties = getNamedQuantityTypes(observationOfferingType, station)

    val sourceObservedProperty = foreignObservatedProperties.flatMap(foreignObservatedProperty => 
      getObservedProperty(foreignObservatedProperty._1, foreignObservatedProperty._2, source))

      logger.info("returning " + sourceObservedProperty.size + " properties")
    return sourceObservedProperty
  }
  
  private def getObservedProperty(aggregateName: String, namedQuantity: NamedQuantityType,
      source: Source): Option[ObservedProperty] = {
      logger.info("getting observed property: " + aggregateName + " - " + namedQuantity.getName())
    (aggregateName, namedQuantity.getName()) match {
      case (_, "WaterTemperature") => {
          return defineObservedProperty(Phenomena.instance.SEA_WATER_TEMPERATURE, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.SEA_WATER_TEMPERATURE))
      }
      case (_, "SignificantWaveHeight") => {
          return defineObservedProperty(Phenomena.instance.SIGNIFICANT_WAVE_HEIGHT, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.SIGNIFICANT_WAVE_HEIGHT))
      }
      case (_, "DominantWavePeriod") => {
          return defineObservedProperty(Phenomena.instance.PEAK_WAVE_PERIOD, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.DOMINANT_WAVE_PERIOD))
      }
      case (_, "AverageWavePeriod") => {
          return defineObservedProperty(Phenomena.instance.MEAN_WAVE_PERIOD, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.AVERAGE_WAVE_PERIOD))
      }
      case (_, "SwellHeight") => {
          return defineObservedProperty(Phenomena.instance.SEA_SURFACE_SWELL_WAVE_SIGNIFICANT_HEIGHT, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.SWELL_HEIGHT))
      }
      case (_, "SwellPeriod") => {
          return defineObservedProperty(Phenomena.instance.SEA_SURFACE_SWELL_WAVE_PERIOD, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.SWELL_PERIOD))
      }
      case (_, "WindWaveHeight") => {
          return defineObservedProperty(Phenomena.instance.SEA_SURFACE_WIND_WAVE_SIGNIFICANT_HEIGHT, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.WIND_WAVE_HEIGHT))
      }
      case (_, "WindWavePeriod") => {
          return defineObservedProperty(Phenomena.instance.SEA_SURFACE_WIND_WAVE_PERIOD, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.WIND_WAVE_PERIOD))
      }
      case (_, "SwellWaveDirection") => {
          return defineObservedProperty(Phenomena.instance.SEA_SURFACE_SWELL_WAVE_TO_DIRECTION, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.SWELL_WAVE_DIRECTION))
      }
      case (_, "WindWaveDirection") => {
        return defineObservedProperty(Phenomena.instance.SEA_SURFACE_WIND_WAVE_TO_DIRECTION, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.WIND_WAVE_DIRECTION))
      }
      case (_, "WindSpeed") => {
        return defineObservedProperty(Phenomena.instance.WIND_SPEED, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.WIND_SPEED))
      }
      case (_, "WindDirection") => {
        return defineObservedProperty(Phenomena.instance.WIND_FROM_DIRECTION, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, convertUnits(namedQuantity.getUom), 
//            SensorPhenomenonIds.WIND_DIRECTION))
      }
      case (_, "WindVerticalVelocity") => {
        return defineObservedProperty(Phenomena.instance.WIND_VERTICAL_VELOCITY, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.WIND_VERTICAL_VELOCITY))
      }
      case (_, "WindGust") => {
        return defineObservedProperty(Phenomena.instance.WIND_GUST_FROM_DIRECTION, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.WIND_GUST))
      }
      case ("http://mmisw.org/ont/cf/parameter/sea_surface_height_amplitude_due_to_equilibrium_ocean_tide", "WaterLevel") => {
        return defineObservedProperty(Phenomena.instance.WATER_SURFACE_HEIGHT_ABOVE_REFERENCE_DATUM, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.WATER_LEVEL_PREDICTIONS))
      }
      case ("http://mmisw.org/ont/cf/parameter/water_surface_height_above_reference_datum", "WaterLevel") => {
        return defineObservedProperty(Phenomena.instance.WATER_SURFACE_HEIGHT_ABOVE_REFERENCE_DATUM, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.WATER_LEVEL))
      }
      case (_, "Salinity") => {
        return defineObservedProperty(Phenomena.instance.SALINITY, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.SALINITY))
      }
      case (_, "Depth") => {
        return defineObservedProperty(Phenomena.instance.SEA_FLOOR_DEPTH_BELOW_SEA_SURFACE, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.SEA_FLOOR_DEPTH_BELOW_SEA_SURFACE))
      }
      case (_, "CurrentDirection") => {
        return defineObservedProperty(Phenomena.instance.CURRENT_DIRECTION, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.CURRENT_DIRECTION))
      }
      case (_, "CurrentSpeed") => {
        return defineObservedProperty(Phenomena.instance.CURRENT_SPEED, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.CURRENT_SPEED))
      }
      case (_, "AirTemperature") => {
        return defineObservedProperty(Phenomena.instance.AIR_TEMPERATURE, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.AIR_TEMPERATURE)) 
      }
      case (_, "BarometricPressure") => {
        return defineObservedProperty(Phenomena.instance.AIR_PRESSURE, aggregateName)
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(namedQuantity.getName,
//            source, namedQuantity.getUom, 
//            SensorPhenomenonIds.BAROMETRIC_PRESSURE)) 
      }
      case (_, "SamplingRate") => None
      case (_, "WaveDuration") => None
      case (_, "MeanWaveDirectionPeakPeriod") => None
      case _ =>{
        logger.debug(" observed property: " + namedQuantity.getName +
          " is not processed correctly.")
        return None
      }
    }
  }
  
  private def defineObservedProperty(phenomenon: Phenomenon, foreignTag: String) : Option[ObservedProperty] = {
    val index = phenomenon.getId().lastIndexOf("/") + 1
    val tag = phenomenon.getId().substring(index)
    var localPhenomenon = new LocalPhenomenon(new DatabasePhenomenon(tag),stationQuery)
    var dbId = -1L
    if (localPhenomenon.getDatabasePhenomenon == null || localPhenomenon.getDatabasePhenomenon.id < 0) {
      dbId = insertPhenomenon(new DatabasePhenomenon(tag), phenomenon.getUnit.getSymbol, phenomenon.getName, phenomenon.getId).id
    } else {
      dbId = localPhenomenon.getDatabasePhenomenon.id
    }
    if (dbId < 0) {
      logger.warn("dbId of -1: " + foreignTag)
      return None
    }
    return new Some[ObservedProperty](stationUpdater.createObservedProperty(foreignTag,source,phenomenon.getUnit.getSymbol,dbId,0d))
  }
  
  private def insertPhenomenon(dbPhenom: DatabasePhenomenon, units: String, description: String, name: String) : DatabasePhenomenon = {
    stationQuery.createPhenomenon(dbPhenom)
  }
  
  private def convertUnits(originalUnits:String)=
    originalUnits match{
      case "deg" => "degrees"
      case x => x
    }

  private def createSouceStation(observationOffering: ObservationOfferingType, 
      source: Source): DatabaseStation = {
    
    val procedure = observationOffering.getProcedureArray(0)
    val stationPostfixName = procedure.getHref().replace("urn:ioos:station:", "")
    val label = observationOffering.getDescription().getStringValue
    val (lat, lon) = getLatLon(observationOffering)

    logger.info("Processing station: " + label)
    return new DatabaseStation(label, stationPostfixName, stationPostfixName, 
        "", "BUOY", source.id, lat, lon)
  }

  private def getLatLon(observationOfferingType: ObservationOfferingType): (Double, Double) = {
    val latLon = observationOfferingType.getBoundedBy().getEnvelope().getLowerCorner().getListValue
    val lat = latLon.get(0).toString().toDouble
    val lon = latLon.get(1).toString().toDouble
    
    return (lat,lon)
  }

  private def getNamedQuantityTypes(observationOfferingType: ObservationOfferingType,
    station: DatabaseStation): List[(String, NamedQuantityType)] = {
    val sensorForeignIds = getSensorForeignIds(observationOfferingType)

    val namedQuantityTypes = for (phenomenon <- sensorForeignIds) yield {
      val rawData = rawDataRetriever.getRawDataLatest(serviceUrl,
        station.foreign_tag, phenomenon)

      createCompositeObservationDocument(rawData) match {
        case Some(document) => {
          try {
            val namedQuantityTypes = getPropertyNames(document).map(propertyName => (phenomenon, propertyName))
            namedQuantityTypes
          } catch {
            case e: Exception => {
              logger.error(" station ID: " + station.foreign_tag +
                " phenomenon: " + phenomenon + " error: parsing data ")
              Nil
            }
          }
        }
        case None => {
          Nil
        }
      }
    }

    return namedQuantityTypes.flatten
  }

  private def getPropertyNames(document: CompositeObservationDocument): List[NamedQuantityType] = {
    val compositePropertyType: CompositePropertyType =
      document.getObservation().getResult().asInstanceOf[CompositePropertyType]

    val valueArrayType: ValueArrayType =
      compositePropertyType.getComposite().getValueComponents().
        getAbstractValueArray(1).asInstanceOf[ValueArrayType]

    val compositeValueType: CompositeValueType =
      valueArrayType.getValueComponents().getAbstractValueArray(0).
        asInstanceOf[CompositeValueType]

    val valueArrayType1: ValueArrayType =
      compositeValueType.getValueComponents().getAbstractValueArray(1).
        asInstanceOf[ValueArrayType]

    val propertyNames = for {
      xmlObject <- valueArrayType1.getValueComponents().getAbstractValueArray();
      val compositeValue = xmlObject.asInstanceOf[CompositeValueType]
    } yield {
      compositeValue.getValueComponents().getAbstractValueArray(1) match {
        case compositeValueType: CompositeValueType => {
          val values = compositeValueType.getValueComponents().getAbstractValueArray().collect {
            case namedQuantity: NamedQuantityType => namedQuantity
          }
          values.toList
        }
        case _ => Nil
      }
    }

    propertyNames.flatten.distinct.toList
  }
  
  private def createCompositeObservationDocument(data: String): 
      Option[CompositeObservationDocument] = {
    if (data != null && !data.contains("ExceptionReport")) {
      try {
        val compositeObservationDocument =
          CompositeObservationDocument.Factory.parse(data)

        return Some[CompositeObservationDocument](compositeObservationDocument)
      } catch {
        case e: Exception => //println("--------------\n" + data) //do nothing
      }
    }

    return None
  }

  private def getSensorForeignIds(observationOfferingType: ObservationOfferingType): List[String] = {
    val sensorForeignIds = observationOfferingType.getObservedPropertyArray().map(_.getHref)
    
    val filteredSensorForeignIds = sensorForeignIds.filter(sensorForeignId => 
      !sensorForeignNotUsed.contains(sensorForeignId))

    return filteredSensorForeignIds.toList
  }
}
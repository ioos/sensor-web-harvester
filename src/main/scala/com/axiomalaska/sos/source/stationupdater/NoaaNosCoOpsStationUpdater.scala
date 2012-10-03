package com.axiomalaska.sos.source.stationupdater

import org.apache.log4j.Logger

import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.source.observationretriever.SosRawDataRetriever
import com.axiomalaska.sos.source.data.Source
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.source.data.SensorPhenomenonIds
import com.axiomalaska.sos.data.Location

import net.opengis.sos.x10.ObservationOfferingType
import net.opengis.sos.x10.CapabilitiesDocument
import net.opengis.sos.x10.GetCapabilitiesDocument
import net.opengis.om.x10.CompositeObservationDocument
import net.opengis.gml.x32.ValueArrayPropertyType

import gov.noaa.ioos.x061.NamedQuantityType
import gov.noaa.ioos.x061.CompositePropertyType
import gov.noaa.ioos.x061.CompositeValueType
import gov.noaa.ioos.x061.ValueArrayType

class NoaaNosCoOpsStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val httpSender = new HttpSender()
  private val geoTools = new GeoTools()
  private val stationIdParser = """station-(\w+)""".r
  private val observedPropteryRefParser = """.*/(\w+)""".r
  private val rawDataRetriever = new SosRawDataRetriever()
  private val serviceUrl = "http://opendap.co-ops.nos.noaa.gov/ioos-dif-sos/SOS"
  private val offeringTag = "urn:x-noaa:def:station:NOAA.NOS.CO-OPS::"
  private val observedPropertyTag = "http://mmisw.org/ont/cf/parameter/"
  private val source = stationQuery.getSource(SourceId.NOAA_NOS_CO_OPS)

  // ---------------------------------------------------------------------------
  // StationUpdater Members
  // ---------------------------------------------------------------------------
  
  def update() {
    val sourceStationSensors = getSourceStations(source)

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }
  
  val name = "NOAA NOS CO-OPS"
  
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
        capabilitiesDocument.getCapabilities().getContents().getObservationOfferingList().getObservationOfferingArray().
          filter(observationOffering =>
            stationIdParser.findFirstIn(observationOffering.getId) != None).toList
      }
      case None => {
        Nil
      }
    }
  }
  
  private def getCapabilitiesDocument(): Option[CapabilitiesDocument] = {
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
    val namedQuantityTypes = getNamedQuantityTypes(observationOfferingType, station)

    val sourceObservedProperty = namedQuantityTypes.flatMap(namedQuantity => 
      getObservedProperty(namedQuantity, source))

    return sourceObservedProperty
  }
  
  private def getObservedProperty(namedQuantity: NamedQuantityType, 
      source: Source): Option[ObservedProperty] = {
    namedQuantity.getName match {
      case "WaterTemperature" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.SEA_WATER_TEMPERATURE))
      }
      case "SignificantWaveHeight" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.SIGNIFICANT_WAVE_HEIGHT))
      }
      case "DominantWavePeriod" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.DOMINANT_WAVE_PERIOD))
      }
      case "AverageWavePeriod" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.AVERAGE_WAVE_PERIOD))
      }
      case "SwellHeight" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.SWELL_HEIGHT))
      }
      case "SwellPeriod" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.SWELL_PERIOD))
      }
      case "WindWaveHeight" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.WIND_WAVE_HEIGHT))
      }
      case "WindWavePeriod" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.WIND_WAVE_PERIOD))
      }
      case "SwellWaveDirection" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.SWELL_WAVE_DIRECTION))
      }
      case "WindWaveDirection" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.WIND_WAVE_DIRECTION))
      }
      case "WindSpeed" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.WIND_SPEED))
      }
      case "WindDirection" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, convertUnits(namedQuantity.getUom), 
            SensorPhenomenonIds.WIND_DIRECTION))
      }
      case "WindVerticalVelocity" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.WIND_VERTICAL_VELOCITY))
      }
      case "WindGust" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.WIND_GUST))
      }
      case "WaterLevel" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.WATER_LEVEL))
      }
      case "Salinity" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.SALINITY))
      }
      case "Depth" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.SEA_FLOOR_DEPTH_BELOW_SEA_SURFACE))
      }
      case "CurrentDirection" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.CURRENT_DIRECTION))
      }
      case "CurrentSpeed" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.CURRENT_SPEED))
      }
      case "AirTemperature" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.AIR_TEMPERATURE)) 
      }
      case "BarometricPressure" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(namedQuantity.getName,
            source, namedQuantity.getUom, 
            SensorPhenomenonIds.BAROMETRIC_PRESSURE)) 
      }
      case "SamplingRate" => None
      case "WaveDuration" => None
      case "MeanWaveDirectionPeakPeriod" => None
      case _ =>{
        logger.debug(" observed property: " + namedQuantity.getName +
          " is not processed correctly.")
        return None
      }
    }
  }
  
  private def convertUnits(originalUnits:String)=
    originalUnits match{
      case "deg" => "degrees"
      case x => x
    }

  private def createSouceStation(observationOfferingType: ObservationOfferingType, 
      source: Source): DatabaseStation = {
    val stationIdParser(foreignId) = observationOfferingType.getId
    val label = observationOfferingType.getDescription().getStringValue
    val (lat, lon) = getLatLon(observationOfferingType)

    logger.info("Processing station: " + label)
    return new DatabaseStation(label, foreignId, foreignId, "", "BUOY", source.id, lat, lon)
  }

  private def getLatLon(observationOfferingType: ObservationOfferingType): (Double, Double) = {
    val latLon = observationOfferingType.getBoundedBy().getEnvelope().getLowerCorner().getListValue
    val lat = latLon.get(0).toString().toDouble
    val lon = latLon.get(1).toString().toDouble
    
    return (lat,lon)
  }

  private def getNamedQuantityTypes(observationOfferingType: ObservationOfferingType, 
      station: DatabaseStation): List[NamedQuantityType] = {
    val sensorForeignIds = getSensorForeignIds(observationOfferingType)
      
    val namedQuantityTypes = for (sensorForeignId <- sensorForeignIds) yield {
      val rawData = rawDataRetriever.getRawDataLatest(serviceUrl, offeringTag, observedPropertyTag,
        station.foreign_tag, sensorForeignId)

        if(rawData == null){
          return Nil
        }
      createCompositeObservationDocument(rawData) match {
        case Some(document) => {
          try {
            val compositePropertyType: CompositePropertyType =
              document.getObservation().getResult().asInstanceOf[CompositePropertyType]

            val valueArrayType: ValueArrayType =
              compositePropertyType.getComposite().getValueComponents().getAbstractValueArray(1).asInstanceOf[ValueArrayType]

            val compositeValueType: CompositeValueType =
              valueArrayType.getValueComponents().getAbstractValueArray(0).asInstanceOf[CompositeValueType]

            val valueArrayType1: ValueArrayType =
              compositeValueType.getValueComponents().getAbstractValueArray(1).asInstanceOf[ValueArrayType]

            val propertyNames = for {
              xmlObject <- valueArrayType1.getValueComponents().getAbstractValueArray();
              val compositeValue = xmlObject.asInstanceOf[CompositeValueType]
              namedQuantityType <- getValues(compositeValue.getValueComponents())
            } yield {
              namedQuantityType
            }

            propertyNames.toList.distinct
          } catch {
            case e:Exception => {
              logger.error(" station ID: " + station.foreign_tag + " sensorForeignId: " + sensorForeignId + " error: parsing data ")
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

  private def createCompositeObservationDocument(data: String): 
      Option[CompositeObservationDocument] = {
    if (!data.contains("ExceptionReport")) {
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

  private def getValues(valuePropertyType: ValueArrayPropertyType): List[NamedQuantityType] = {
    valuePropertyType.getAbstractValueArray(1) match {
      case compositeValueType: CompositeValueType => {
        val values = compositeValueType.getValueComponents().getAbstractValueArray().collect {
          case namedQuantity: NamedQuantityType => namedQuantity
        }

        values.toList
      }
      case _ => Nil
    }
  }
  
  private def sensorForeignNotUsed = List("harmonic_constituents", "datums")

  private def getSensorForeignIds(observationOfferingType: ObservationOfferingType): List[String] = {
    val sensorForeignIds = observationOfferingType.getObservedPropertyArray().map(o => {
      val observedPropteryRefParser(property) = o.getHref
      property
    })
    
    val filteredSensorForeignIds = sensorForeignIds.filter(sensorForeignId => !sensorForeignNotUsed.contains(sensorForeignId))

    return filteredSensorForeignIds.toList
  }
}
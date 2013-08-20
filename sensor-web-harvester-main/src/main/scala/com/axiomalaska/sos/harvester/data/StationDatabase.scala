package com.axiomalaska.sos.harvester.data

import org.squeryl.Schema
import org.squeryl.KeyedEntity
import org.squeryl.PrimitiveTypeMode._
import org.squeryl.dsl.OneToMany
import org.squeryl.dsl.ManyToOne
import org.squeryl.dsl.CompositeKey2
import org.squeryl.Query
import org.squeryl.annotations.Column
import java.sql.Timestamp

object StationDatabase extends Schema {

  val sources = table[Source]("source")
  val stations = table[DatabaseStation]("station")
  val sensors = table[DatabaseSensor]("sensor")
  val phenomena = table[DatabasePhenomenon]("phenomenon")
  val observedProperties = table[ObservedProperty]("observed_property")
  val networks = table[Network]("network")
  
  val sourceStationAssociation = 
    oneToManyRelation(sources, stations).via(
        (source, station) => station.source_id === source.id)
        
  val stationSensorAssociation = 
    oneToManyRelation(stations, sensors).via(
        (station, sensor) => sensor.station_id === station.id)
        
  val sourceObservedPropertyAssociation = 
    oneToManyRelation(sources, observedProperties).via(
        (source, observedProperty) => observedProperty.source_id === source.id)
        
  val phenomenonObservedPropertyAssociation = 
    oneToManyRelation(phenomena, observedProperties).via(
        (phenomenon, observedProperty) => observedProperty.phenomenon_id === phenomenon.id)
  
  val xSensorPhenomenon = 
    manyToManyRelation(sensors, phenomena).via[sensor_phenomenon](
        (sensor, phenomenon, x) => (sensor.id === x.sensor_id, x.phenomenon_id === phenomenon.id))
        
  val xNetworkSource =
    manyToManyRelation(networks, sources).via[network_source](
        (network, source, x) => (network.id === x.network_id, x.source_id === source.id))
        
  val xNetworkStation = 
    manyToManyRelation(networks, stations).via[network_station](
        (network, station, x) => (network.id === x.network_id, x.station_id === station.id))
        
  on(networks)(network => declare(network.id is 
      (autoIncremented("network_id_seq"))))
      
  on(observedProperties)(observedProperty => declare(observedProperty.id is 
      (autoIncremented("observed_property_id_seq"))))
      
  on(phenomena)(phenomenon => declare(phenomenon.id is 
      (autoIncremented("phenomenon_id_seq"))))
      
  on(stations)(station => declare(station.id is 
      (autoIncremented("station_id_seq"))))
      
  on(sources)(source => declare(source.id is 
      (autoIncremented("source_id_seq"))))
      
  on(sensors)(sensor => declare(sensor.id is 
      (autoIncremented("sensor_id_seq"))))
}

class Network(val tag: String, val description:String, 
    @Column("source_tag") val sourceTag:String, 
    @Column("long_name") val longName:String, 
    @Column("short_name")val shortName:String) extends KeyedEntity[Long] {
  val id: Long = -1
  
  lazy val sources:Query[Source] = StationDatabase.xNetworkSource.left(this)
  lazy val stations:Query[DatabaseStation] = StationDatabase.xNetworkStation.left(this)
}

class Source(val name: String, val tag:String, val country:String, 
    val email:String, @Column("web_address") val webAddress:String, 
    @Column("operator_sector") val operatorSector:String, val address:String, 
    val city:String, val state:String, val zipcode:String) extends KeyedEntity[Long] {
  val id: Long = -1
  
  lazy val stations: OneToMany[DatabaseStation] = 
    StationDatabase.sourceStationAssociation.left(this)
    
  lazy val observedProperties: OneToMany[ObservedProperty] = 
    StationDatabase.sourceObservedPropertyAssociation.left(this)
    
  lazy val networks:Query[Network] = StationDatabase.xNetworkSource.right(this)
}

class DatabaseStation(val name: String, val tag:String, val foreign_tag: String,
  val description:String, @Column("platform_type") val platformType:String, 
  val source_id: Long, val latitude: Double, val longitude: Double, val active:Boolean,
  @Column("time_begin") val timeBegin: Timestamp, @Column("time_end") val timeEnd: Timestamp)
  extends KeyedEntity[Long] {
  val id: Long = -1
  
  def this(name: String, tag:String, foreign_tag: String, description:String, 
      platformType:String, source_id: Long, latitude: Double, longitude: Double,
      timeBegin: Timestamp, timeEnd: Timestamp){
    this(name, tag, foreign_tag, description, platformType, source_id,
        latitude, longitude, true, timeBegin, timeEnd)
  }

  lazy val source: ManyToOne[Source] = 
    StationDatabase.sourceStationAssociation.right(this)
    
  lazy val sensors: OneToMany[DatabaseSensor] = 
    StationDatabase.stationSensorAssociation.left(this)
    
  lazy val networks:Query[Network] = StationDatabase.xNetworkStation.right(this)
}

class DatabaseSensor(val tag: String, val description:String, val station_id:Long, 
    val active:Boolean) extends KeyedEntity[Long] {
  val id: Long = -1
  
  def this(tag: String, description:String, station_id:Long){
    this(tag, description, station_id, true)
  }
  
  lazy val phenomena = 
    StationDatabase.xSensorPhenomenon.left(this)
  
  lazy val station:ManyToOne[DatabaseStation] = 
    StationDatabase.stationSensorAssociation.right(this)
}

// I know that we are suppose to move away from this, but I can't find any other way of inserting a phenom into the DB, will continue
// to use this until something new is available
class DatabasePhenomenon(val tag:String, val units:String) extends KeyedEntity[Long]{
  val id: Long = -1

  lazy val sensors:Query[DatabaseSensor] = StationDatabase.xSensorPhenomenon.right(this)
  
  lazy val observedProperties: OneToMany[ObservedProperty] = 
    StationDatabase.phenomenonObservedPropertyAssociation.left(this)
}

class ObservedProperty(val foreign_tag: String,
  val source_id: Long, val foreign_units: String, val phenomenon_id: Long, 
  val depth:Double) extends KeyedEntity[Long] {
  val id: Long = -1
  
  def this(foreign_tag: String, source_id: Long, foreign_units: String, 
      phenomenon_id: Long){
    this(foreign_tag, source_id, foreign_units, phenomenon_id, 0)
  }
  
  lazy val source: ManyToOne[Source] = 
    StationDatabase.sourceObservedPropertyAssociation.right(this)
    
  lazy val phenomenon: ManyToOne[DatabasePhenomenon] = 
    StationDatabase.phenomenonObservedPropertyAssociation.right(this)
}

class sensor_phenomenon(val sensor_id: Long, val phenomenon_id: Long)
  extends KeyedEntity[CompositeKey2[Long, Long]] {
  val id = compositeKey(phenomenon_id, phenomenon_id)
}

class network_station(val network_id: Long, val station_id: Long)
  extends KeyedEntity[CompositeKey2[Long, Long]] {
  val id = compositeKey(network_id, station_id)
}

class network_source(val network_id: Long, val source_id: Long)
  extends KeyedEntity[CompositeKey2[Long, Long]] {
  val id = compositeKey(network_id, source_id)
}

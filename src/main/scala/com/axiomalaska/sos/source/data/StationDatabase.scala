package com.axiomalaska.sos.source.data

import org.squeryl.Schema
import org.squeryl.KeyedEntity
import org.squeryl.PrimitiveTypeMode._
import org.squeryl.dsl.OneToMany
import org.squeryl.dsl.ManyToOne
import org.squeryl.dsl.CompositeKey2
import org.squeryl.Query
import org.squeryl.annotations.Column

object StationDatabase extends Schema {

  val sources = table[Source]("source")
  val stations = table[DatabaseStation]("station")
  val sensors = table[DatabaseSensor]("sensor")
  val phenomena = table[DatabasePhenomenon]("phenomenon")
  val observedProperties = table[ObservedProperty]("observed_property")
  
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

class Source(val name: String, val tag:String, val country:String, 
    val email:String, @Column("web_address") val webAddress:String, 
    @Column("operator_sector") val operatorSector:String, val address:String, 
    val city:String, val state:String, val zipcode:String) extends KeyedEntity[Long] {
  val id: Long = -1
  
  lazy val stations: OneToMany[DatabaseStation] = 
    StationDatabase.sourceStationAssociation.left(this)
    
  lazy val observedProperties: OneToMany[ObservedProperty] = 
    StationDatabase.sourceObservedPropertyAssociation.left(this)
}

class DatabaseStation(val name: String, val tag:String, val foreign_tag: String,
  val description:String, @Column("platform_type") val platformType:String, 
  val source_id: Long, val latitude: Double, val longitude: Double, val active:Boolean) 
  extends KeyedEntity[Long] {
  val id: Long = -1
  
  def this(name: String, tag:String, foreign_tag: String, description:String, 
      platformType:String, source_id: Long, latitude: Double, longitude: Double){
    this(name, tag, foreign_tag, description, platformType, source_id, 
        latitude, longitude, true)
  }
  
  lazy val source: ManyToOne[Source] = 
    StationDatabase.sourceStationAssociation.right(this)
    
  lazy val sensors: OneToMany[DatabaseSensor] = 
    StationDatabase.stationSensorAssociation.left(this)
}

class DatabaseSensor(val tag: String, val description:String, val station_id:Long, 
    val depth:Double, val active:Boolean) extends KeyedEntity[Long] {
  val id: Long = -1
  
  def this(tag: String, description:String, station_id:Long, depth:Double){
    this(tag, description, station_id, depth, true)
  }
  
  def this(tag: String, description:String, station_id:Long){
    this(tag, description, station_id, 0, true)
  }
  
  lazy val phenomena = 
    StationDatabase.xSensorPhenomenon.left(this)
  
  lazy val station:ManyToOne[DatabaseStation] = 
    StationDatabase.stationSensorAssociation.right(this)
}

class DatabasePhenomenon(val tag:String) extends KeyedEntity[Long]{
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

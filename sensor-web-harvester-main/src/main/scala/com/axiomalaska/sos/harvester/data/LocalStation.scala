package com.axiomalaska.sos.harvester.data

import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.data.SosStation
import scala.collection.JavaConversions._
import com.axiomalaska.sos.data.SosNetwork
import scala.collection.mutable
import org.n52.sos.ioos.asset.AssetResolver
import org.n52.sos.ioos.asset.NetworkAsset
import org.n52.sos.ioos.asset.AbstractAsset
import org.n52.sos.ioos.asset.StationAsset
import org.n52.sos.ioos.asset.AssetConstants
import com.axiomalaska.ioos.sos.GeomHelper
import com.axiomalaska.sos.harvester.StationQuery

class LocalStation(val localSource:LocalSource, 
    val databaseStation: DatabaseStation,
  private val stationQuery: StationQuery) extends SosStation {

  setAsset(createStationAsset())
      
  setFeatureOfInterestName("station: " + databaseStation.name + 
      " of source: " + localSource.getName)
      
  setLocation(GeomHelper.createLatLngPoint(
      databaseStation.latitude, databaseStation.longitude))
 
  setNetworks(collectNetworks())
  
  setSource(localSource)
  
  setPlatformType(databaseStation.platformType)
  
  setWmoId("")
  
  setSponsor("")
  
  setLongName(pullLongName)
  setShortName(databaseStation.name)
  
  setSensors(stationQuery.getActiveSensors(databaseStation).map(
      sensor => new LocalSensor(sensor, this, stationQuery)))
  
  private def pullLongName():String = {
    if(databaseStation.description.isEmpty()){
      databaseStation.name
    } else{
      databaseStation.description
    }
  }
  
  private def createStationAsset():StationAsset ={
    val parts = databaseStation.tag.split(":")
    if(parts.size == 2){
    	new StationAsset(parts.head, parts.tail.head)
    } else{
      throw new Exception(
          "the station tag must have the authority and id seperated by a : " +  
          databaseStation.tag)
    }
  }
  
  private def collectNetworks():java.util.List[SosNetwork] ={
    val sourceNetworks = stationQuery.getNetworks(localSource.source).map(
        network => buildSosNetwork(network))
    val stationNetworks = stationQuery.getNetworks(databaseStation).map(
        network => buildSosNetwork(network))
    val combinedNetworks =  sourceNetworks ::: stationNetworks
    val set = new mutable.HashSet[String]

    // get networks from DB
    for {
      network <- combinedNetworks
      val networkId = network.getAsset().getAssetId()
      if (!set.contains(networkId))
    } yield {
      set += networkId
      network
    }
    Nil
  }
    
  private def buildSosNetwork(databaseNetwork:Network):SosNetwork ={
    val networkAsset = new NetworkAsset(databaseNetwork.sourceTag, databaseNetwork.tag)

    val network = new SosNetwork()
    network.setAsset(networkAsset)
    network.setLongName(databaseNetwork.longName)
    network.setShortName(databaseNetwork.shortName)
    
    network
  }
}

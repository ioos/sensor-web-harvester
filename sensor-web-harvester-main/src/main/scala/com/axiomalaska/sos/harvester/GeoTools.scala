package com.axiomalaska.sos.harvester
import com.vividsolutions.jts.geom.Point
import org.geotools.data.DataStoreFinder
import java.util.HashMap
import java.net.URL
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import scala.collection.mutable.{Set => MSet}
import scala.collection.mutable.{Set => MSet}

case class BoundingBox(southWestCorner: Point, northEastCorner: Point)

object GeoTools {
  val stateFeatureSource = initStateFeatureSource
  
  def initStateFeatureSource:SimpleFeatureSource = {
    val map = new HashMap[String,URL]
    val url = getClass.getClassLoader.getResource("states/ne_110m_admin_1_states_provinces_lakes_shp.shp")
    map.put("url", url)
    val dataStore = DataStoreFinder.getDataStore(map)
    val typeNames = dataStore.getTypeNames
    dataStore.getFeatureSource(typeNames(0))    
  }
  
  def statesInBoundingBox(boundingBox:BoundingBox):Set[String] = {
    val ff = CommonFactoryFinder.getFilterFactory2()
    val bbox = new ReferencedEnvelope(
        boundingBox.southWestCorner.getX, boundingBox.northEastCorner.getX,
        boundingBox.southWestCorner.getY, boundingBox.northEastCorner.getY,
        DefaultGeographicCRS.WGS84)    
    val filter = ff.bbox(ff.property("the_geom"), bbox);
    val simpleFeatureCollection = stateFeatureSource.getFeatures(filter)
    val stateIterator = simpleFeatureCollection.features
    val states = MSet.empty[String]
    while (stateIterator.hasNext) { 
      val feature = stateIterator.next
      feature.getAttribute("name").toString() match {
        case s: String => states += s
      }
    }
    states.toSet
  }
}

class GeoTools {
	/**
	 * Test if a Location is within the bounding box setup by the North West Corner
	 * and the South West corner. 
	 * 
	 * @param stationLocation - the station location to test with the bounding box
	 * @param boundingBox - the bounding box to station location
	 * @return true if within bounding box, false otherwise
	 */
	def isStationWithinRegion(stationLocation:Point, 
			boundingBox:BoundingBox):Boolean = {
		if(boundingBox.northEastCorner != null && boundingBox.southWestCorner != null){
			if(doesBoundingBoxGoOver180Longitude(boundingBox)){
				isStationWithinRegionOver180Longitude(stationLocation, 
						boundingBox)
			}
			else{
				isStationWithinRegionNormal(stationLocation, 
						boundingBox)
			}
		}
		else{
			true;
		}
	}
	
	// -------------------------------------------------------------------------
	// Private Members
	// -------------------------------------------------------------------------
	
	/**
	 * If the south west longitude is greater than the north east longitude, then
	 * the bounding box crosses over 180 longitude. 
	 * 
	 * @param boundingBox - the bounding box to station location
	 * @return
	 */
	private def doesBoundingBoxGoOver180Longitude(boundingBox:BoundingBox):Boolean ={
		(boundingBox.southWestCorner.getX() > boundingBox.northEastCorner.getX())
	}
	
	private def isStationWithinRegionNormal(stationLocation:Point,  
			boundingBox:BoundingBox):Boolean = {
		if(stationLocation.getY() <= boundingBox.northEastCorner.getY() && 
		   stationLocation.getY() >= boundingBox.southWestCorner.getY() && 
		   stationLocation.getX() <= boundingBox.northEastCorner.getX() && 
		   stationLocation.getX() >= boundingBox.southWestCorner.getX() ){
			return true;
		}
		else{
			return false;
		}
	}
	
	private def isStationWithinRegionOver180Longitude(stationLocation:Point,
			boundingBox:BoundingBox):Boolean = {
		if (stationLocation.getX() > 0) {
			if (stationLocation.getY() <= boundingBox.northEastCorner.getY()
				&& stationLocation.getY() >= boundingBox.southWestCorner.getY()
				&& stationLocation.getX() > boundingBox.southWestCorner.getX()) {
				return true;
			} else {
				return false;
			}
		} else {
			if (stationLocation.getY() <= boundingBox.northEastCorner.getY()
			   && stationLocation.getY() >= boundingBox.southWestCorner.getY()
			   && stationLocation.getX() <= boundingBox.northEastCorner.getX()) {
				return true;
			} else {
				return false;
			}
		}
	}
}
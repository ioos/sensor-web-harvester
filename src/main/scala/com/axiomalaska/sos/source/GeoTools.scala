package com.axiomalaska.sos.source

import com.axiomalaska.sos.data.SosStation
import com.vividsolutions.jts.geom.Point

case class BoundingBox(southWestCorner: Point, northEastCorner: Point)

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
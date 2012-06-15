package com.axiomalaska.aoosSosInjector

import org.junit._
import Assert._
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.data.SosStationImp
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.source.BoundingBox

@Test
class GeoToolsTest {
	@Test
	def testGeotoolsNormal() {
		val geotools = new GeoTools();
		
		val southWestCorner = new Location(40, -170);
		val northEastCorner = new Location(60, -130);
		val boundingBox = BoundingBox(southWestCorner, northEastCorner)
		
		var stationLocation = new Location(50, -160);
		
		assertTrue(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = new Location(50, -140);
		
		assertTrue(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = new Location(30, -140);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = new Location(70, -140);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox))
		
		stationLocation = new Location(50, -175);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox))
		
		stationLocation = new Location(50, -120);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox))
	}
	
	@Test
	def testGeotoolsBoundingBoxAround180Degrees() {
		val geotools = new GeoTools();
		var stationLocation = new Location(50, 170);
		
		val southWestCorner = new Location(40, 160);
		val northEastCorner = new Location(60, -160);
		val boundingBox = BoundingBox(southWestCorner, northEastCorner)
		
		assertTrue(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = new Location(50, -170);
		
		assertTrue(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = new Location(50, -161);
		
		assertTrue(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = new Location(50, -159);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = new Location(50, 159);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = new Location(70, 170);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = new Location(70, -170);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = new Location(30, -170);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = new Location(30, 170);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
	}
}
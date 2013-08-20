package com.axiomalaska.sos.harvester

import org.junit._
import Assert._
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.ioos.sos.GeomHelper

@Test
class GeoToolsTest {
	@Test
	def testGeotoolsNormal() {
		val geotools = new GeoTools();
		
		val southWestCorner = GeomHelper.createLatLngPoint(40, -170);
		val northEastCorner = GeomHelper.createLatLngPoint(60, -130);
		val boundingBox = BoundingBox(southWestCorner, northEastCorner)
		
		var stationLocation = GeomHelper.createLatLngPoint(50, -160);
		
		assertTrue(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = GeomHelper.createLatLngPoint(50, -140);
		
		assertTrue(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = GeomHelper.createLatLngPoint(30, -140);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = GeomHelper.createLatLngPoint(70, -140);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox))
		
		stationLocation = GeomHelper.createLatLngPoint(50, -175);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox))
		
		stationLocation = GeomHelper.createLatLngPoint(50, -120);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox))
	}
	
	@Test
	def testGeotoolsBoundingBoxAround180Degrees() {
		val geotools = new GeoTools();
		var stationLocation = GeomHelper.createLatLngPoint(50, 170);
		
		val southWestCorner = GeomHelper.createLatLngPoint(40, 160);
		val northEastCorner = GeomHelper.createLatLngPoint(60, -160);
		val boundingBox = BoundingBox(southWestCorner, northEastCorner)
		
		assertTrue(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = GeomHelper.createLatLngPoint(50, -170);
		
		assertTrue(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = GeomHelper.createLatLngPoint(50, -161);
		
		assertTrue(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = GeomHelper.createLatLngPoint(50, -159);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = GeomHelper.createLatLngPoint(50, 159);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = GeomHelper.createLatLngPoint(70, 170);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = GeomHelper.createLatLngPoint(70, -170);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = GeomHelper.createLatLngPoint(30, -170);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
		
		stationLocation = GeomHelper.createLatLngPoint(30, 170);
		
		assertFalse(geotools.isStationWithinRegion(
				stationLocation, boundingBox));
	}
	
    @Test
    def testStatesInBoundingBox() {
        val southWestCorner = GeomHelper.createLatLngPoint(40, -170);
        val northEastCorner = GeomHelper.createLatLngPoint(60, -130);
        val boundingBox = BoundingBox(southWestCorner, northEastCorner)
        
        val states = GeoTools.statesInBoundingBox(boundingBox)
        assertTrue(states.size == 1)
        assertEquals(states.head, "Alaska")
    }
}
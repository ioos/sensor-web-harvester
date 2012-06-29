#source-sos-injectors#
====================
The source-sos-injectors project implements several common sources with the [SosInjector 
project](https://github.com/axiomalaska/sos-injection). This project allows one 
to inject these sources' observations into an SOS. 

The current sources that are implemented are:

* [HADS](http://dipper.nws.noaa.gov/hdsc/pfds/)
* [NDBC](http://www.ndbc.noaa.gov/)
* [NOAA NOS CO-OPS](http://tidesonline.nos.noaa.gov/)
* [NOAA Weather](http://www.nws.noaa.gov/)
* [RAWS](http://www.raws.dri.edu/)
* [SnoTel](http://www.wcc.nrcs.usda.gov/)
* [USGS Water](http://waterdata.usgs.gov/ak/nwis/uv)

This project can be used from the command line with the commands of 

To update the metadata database with all the stations from all the sources within the bounding box
This should be only about 3 times a week, because the stations do not change often
java -jar source-sos-injectors.jar -metadata [databaseUrl] [databaseUsername] [databasePassword] [North most latitude] [South most latitude] [West most longitude] [East most longitude]
	
To update the SOS with all the station in the metadata database. Call this hourly or more
java -jar source-sos-injectors.jar -updatesos [SOS URL] [databaseUrl] [databaseUsername] [databasePassword]

Code (Java)
-----------

    // Southern California Bounding Box
    Location southWestCorner = new Location(32.0, -123.0);
    Location northEastCorner = new Location(35.0, -113.0);
    BoundingBox boundingBox = new BoundingBox(southWestCorner, northEastCorner);
    
    String databaseUrl = "jdbc:postgresql://localhost:5432/sensor";
    String databaseUsername = "sensoruser";
    String databasePassword = "sensor";
    String sosUrl = "http://localhost:8080/sos/sos";
    
    MetadataDatabaseManager metadataManager = new MetadataDatabaseManager(databaseUrl, 
    	databaseUsername, databasePassword, boundingBox)
    
    // Updates the local metadata database with station information
    // This call should only be made every couple days a week. 
    metadataManager.update();
    
    SosSourcesManager sosManager = new SosSourceManager(databaseUrl, 
    	databaseUsername, databasePassword, sosUrl);
    	
    // Updates the SOS with data pulled from the source sites. 
    // This uses the metadata database
    // A lot of the data is hourly. Call this hourly or more. 
    sosManager.updateSos();
    

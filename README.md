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

Setup 
-----
A sensor database must be created with the sensor_database_0.0.1.tar. This tar file
contains a backup of the postgresql metadata database used to update the SOS. Using
pgAdmin, create a database right-click on it and select restore. Select the 
sensor_database_0.0.1.tar file for the Filename and for "Format" select "Custom or tar". 
Select "Restore" and the metadata database will be created. This database starts with
all the Phenomena and Sources preloaded. 

To update the metadata database with all the stations from all the sources within the bounding box
This should be done only about 3 times a week, because the stations from the sources do not change often.

java -jar source-sos-injectors.jar -metadata [databaseUrl] [databaseUsername] [databasePassword] [North most latitude] [South most latitude] [West most longitude] [East most longitude]
	
Example:

java -jar source-sos-injectors.jar -metadata "jdbc:postgresql://localhost:5432/sensor" sensoruser sensor 40.7641 32.6666 -124.1245 -114.0830
	
To update the SOS with all the station in the metadata database. Call this hourly or less

java -jar source-sos-injectors.jar -updatesos [SOS URL] [databaseUrl] [databaseUsername] [databasePassword]

Example:

java -jar source-sos-injectors.jar -updatesos "http://localhost:8080/sos/sos" "jdbc:postgresql://localhost:5432/sensor" sensoruser sensor

Code (Java)
-----------
This is example code demonstrating how to update the metadata database and update
the SOS. 

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
    

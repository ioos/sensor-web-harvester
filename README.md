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


This project uses a postgresql metadata database to store station information 
from the sources. This database needs to be built from the provided database 
backup in Github. The station information is used to retrieve observations 
from the stations' sources.

This project needs an already running instance of an 52 North SOS.

Setup 
-----
A sensor database must be created with the sensor_database_0.0.2.tar. This file can be
found in the [Downloads section](https://github.com/axiomalaska/source-sos-injectors/downloads) on Github. This tar file
contains a backup of the postgresql metadata database used to update the SOS. Using
pgAdmin, create a database right-click on it and select restore. Select the 
sensor_database_0.0.1.tar file for the Filename and for "Format" select "Custom or tar". 
Select "Restore" and the metadata database will be created. This database starts with
all the Phenomena and Sources preloaded. 


From Command line
-----------------
To use the command line option and not have to build the code yourself one can download the pre-built jar from 
[Downloads section](https://github.com/axiomalaska/source-sos-injectors/downloads) on Github. 

The command line takes in a properties file that contains all the needed variables to perform the SOS update. 
The properties file needs all the required variables below:
* database_url - the URL where the metadata database can be found. For example jdbc:postgresql://localhost:5432/sensor
* database_username - a user name to access the metadata database
* database_password - the password associated to the database_username
* sos_url - the URL to the SOS being used
* publisher_country - the publisher's country. For example USA
* publisher_email - the publisher's email address
* publisher_web_address - the web address of the publisher. For example www.aoos.org
* publisher_name - the name of the publishing. For example AOOS
* north_lat - the northern most latitude of the bounding box
* south_lat - the southern most latitude of the bounding box
* west_lon - the west most longitude of the bounding box
* east_lon - the east most longitude of the bounding box


To update the metadata database with all the stations from all the sources within the bounding box
This should be done only about 3 times a week, because the stations from the sources do not change often.

java -jar source-sos-injectors.jar -metadata [path to properties file]
	
Example:

java -jar source-sos-injectors.jar -metadata sos.properties
	
To update the SOS with all the station in the metadata database. Do not call this more than once hourly.

java -jar source-sos-injectors.jar -updatesos [path to properties file]

Example:

java -jar source-sos-injectors.jar -updatesos sos.properties

Example of a properties file

database_url = jdbc:postgresql://localhost:5432/sensor
database_username = sensoruser
database_password = sensor
sos_url = http://192.168.8.15:8080/sos/sos
publisher_country = USA
publisher_email = publisher@domain.com
publisher_web_address = http://www.aoos.org/
publisher_name = AOOS
north_lat = 40.0
south_lat = 39.0
west_lon = -80.0
east_lon = -74.0

There is also an example properties file on Github at the [Downloads section](https://github.com/axiomalaska/source-sos-injectors/downloads).

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
    
    // Information about you, the group publishing this data on the SOS. 
    PublisherInfo publisherInfo = new PublisherInfoImp();
    publisherInfo.setCountry("USA");
    publisherInfo.setEmail("publisher@domain.com");
    publisherInfo.setName("IOOS");
    publisherInfo.setWebAddress("http://www.ioos.gov/");
    
    SosSourcesManager sosManager = new SosSourceManager(databaseUrl, 
    	databaseUsername, databasePassword, sosUrl, publisherInfo);
    	
    // Updates the SOS with data pulled from the source sites. 
    // This uses the metadata database
    // A lot of the data is hourly. Call this hourly or more. 
    sosManager.updateSos();
    

#sensor-web-harvester#
====================
sensor-web-harvester is a Scala project that harvests sensor data from web sources. The data is then pushed to an SOS using the [sos-injection module](https://github.com/axiomalaska/sos-injection) project. SosInjector is a project that wraps an [Sensor Observation Service (SOS)](http://52north.org/communities/sensorweb/sos/). The sos-injection module provides Java classes to enter stations, sensors, and observations into an SOS.

sensor-web-harvester is used to fill an SOS with observations from many well-known sensor sources (such as NOAA and NERRS). This project pulls sensor observation values from the source’s stations. It then formats the data to be placed into the user’s SOS by using the sos-injector. The source stations used are filtered by a chosen bounding box area. 

The current sources that observations are pulled from are:

* [HADS](http://dipper.nws.noaa.gov/hdsc/pfds/)
* [NDBC](http://www.ndbc.noaa.gov/)
* [NOAA NOS CO-OPS](http://tidesonline.nos.noaa.gov/)
* [NOAA Weather](http://www.nws.noaa.gov/)
* [RAWS](http://www.raws.dri.edu/)
* [SnoTel](http://www.wcc.nrcs.usda.gov/)
* [USGS Water](http://waterdata.usgs.gov/ak/nwis/uv)
* [NERRS](http://www.nerrs.noaa.gov/)


This project uses a postgresql metadata database to store station information from the sources. This database needs to be built/restored from the provided database backup in Github. The metadata database information is used to retrieve observations from the stations' sources.

This project can be executed by running the pre-built jar with the command line (see “Running the SOS Injector”) or by writing custom Java code (see “Writing Custom Java Code”).

Installation
------------
This project can be used on either a Windows or Linux computer. An Apple computer is expected to work however it has not been tested. 

The following are the requirements to run this project:
* Java 1.6 or newer 
* An already running instance of an [IOOS Customized 52 North SOS](http://ioossostest.axiomalaska.com)
* Postgresql database
* Metadata Database (explained below)

Metadata Database
-----------------
The metadata database is used to collect the stations’ metadata in order to allow observations to be pulled and placed into an SOS. The sensor metadata database must be created using the provided metadata database backup database. This backup database contains all of the phenomena’s and sources’ information, and other tables to be filled later. To install the backup database perform the following steps:
* Download sensor_metadata_database_\[version\].backup from https://github.com/axiomalaska/sensor-web-harvester/tree/master/downloads.

Then restore the backup to a PostgreSQL database.

Using pgadmin:
* Create a database (e.g. sensor-metadata).
* Right-click on this newly created database and select “Restore”.
* Select the sensor_metadata_database_\[version\].backup file for the “Filename” text field.
* In the "Format" combobox select "Custom or tar" item.
* On the Restore Options #1 tab under Don't Save, check Owner.
* Click the "Restore" button.

Using the command line (adjust host, port, user, dbname as needed):

    createdb --host localhost --port 5432 --user postgres sensor-metadata
    pg_restore --clean --dbname sensor-metadata --no-owner --no-tablespace --host localhost --port 5432 --username postgres sensor_metadata_database_\[version\].backup 

Upon completing these steps the metadata database will be created. Record this database’s IP address, port, and name (as seen below) for use later on. 

jdbc:postgresql://[IPAddress]:[port #]/[databasename]

jdbc:postgresql://localhost:5432/sensor-metadata

Running the SOS Injector
-----------
The pre-built sensor-web-harvester.jar and example_sos.properties can be downloaded from the 
[Downloads folder](https://github.com/axiomalaska/sensor-web-harvester/tree/master/downloads) on Github. 

The command line takes in a properties file which contains all of the needed variables to perform an SOS update. The network root is the default network in the SOS that contains all the stations. This network is different for each SOS. For example for AOOS the defaut network is urn:ioos:network:aoos:all. The properties file requires the following variables:
* database_url - the URL where the metadata database can be found (recorded in the above section “Metadata Database”). Example: jdbc:postgresql://localhost:5432/sensor-metadata
* database_username - the username used to access the metadata database
* database_password - the password associated to the database_username
* sos_url - the URL to the SOS being used. Example: http://192.168.1.40:8080/sos/sos
* publisher_country - the publisher's country. Example: USA
* publisher_email - the publisher's email address
* publisher_web_address - the web address of the publisher. Example: www.aoos.org
* publisher_name - the name of the publishing organization. Example: AOOS
* north_lat - the northernmost latitude of the bounding box
* south_lat - the southernmost latitude of the bounding box
* west_lon - the westernmost longitude of the bounding box
* east_lon - the easternmost longitude of the bounding box
* network_root_id - For this root network urn:ioos:network:aoos:all "all" is the root_id
* network_root_source_id - For this root network urn:ioos:network:aoos:all "aoos" is the source_id

**Note that running these processes can take a long time (hours) as information is downloaded and extracted from many sources.**

Use the line below to update the metadata database with all of the stations from the sources within the user-selected bounding box. This command should be run conservatively (approx. 3 times a week) since the sources’ stations do not change often and this command is taxing on the sources’ servers.

    java -jar sensor-web-harvester.jar -metadata [path to properties file]
	
Example: 

    java -jar sensor-web-harvester.jar -metadata sos.properties
	
Use the line below to update the SOS with all of the stations in the metadata database. Do not call this command more than once hourly (for reasons previously stated).

    java -jar sensor-web-harvester.jar -updatesos [path to properties file]

Example:

    java -jar sensor-web-harvester.jar -updatesos sos.properties

Example of a properties file:

    database_url = jdbc:postgresql://localhost:5432/sensor-metadata
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
    network_root_id = all
    network_root_source_id = aoos

An example of a properties file named  “example_sos.properties” is also provided on Github at the [Downloads Folder](https://github.com/axiomalaska/sensor-web-harvester/tree/master/downloads).

Writing Custom Java Code
-----------
This is example code demonstrating how to update the metadata database and the SOS from within custom Java code.

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
    // This call should be made conservatively (approx. 3 times a week) since the sources’ stations do not change often and this call is taxing on the sources’ servers.
    metadataManager.update();
    
    // Information about the group publishing this data on the SOS. 
    PublisherInfoImp publisherInfo = new PublisherInfoImp();
    publisherInfo.setCountry("USA");
    publisherInfo.setEmail("publisher@domain.com");
    publisherInfo.setName("IOOS");
    publisherInfo.setWebAddress("http://www.ioos.gov/");

    SosNetworkImp rootNetwork = new SosNetworkImp()
    rootNetwork.setId("all")
    rootNetwork.setSourceId("aoos")
    
    SosSourcesManager sosManager = new SosSourceManager(databaseUrl, 
    	databaseUsername, databasePassword, sosUrl, publisherInfo, rootNetwork);
    	
    // Updates the SOS with data pulled from the source sites. 
    // This uses the metadata database
    // Most of the data is hourly. The data should be pulled conservatively (approx. hourly) since the observations do not change often and this action is taxing on the sources’ servers.
    sosManager.updateSos();
    


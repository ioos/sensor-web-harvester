CREATE TABLE IF NOT EXISTS source (
    id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL,
    name VARCHAR(255),
    tag VARCHAR(255),
    country VARCHAR(255),
    email VARCHAR(255),
    web_address VARCHAR(255),
    operator_sector VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    zipcode VARCHAR(255)
) AS SELECT * FROM CSVREAD('classpath:source.csv');

CREATE TABLE IF NOT EXISTS network (
    id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL,
    description VARCHAR(255),
    tag VARCHAR(255),
    source_tag VARCHAR(255),
    short_name VARCHAR(255),
    long_name VARCHAR(255)
) AS SELECT ROWNUM(),
  'A network of all the ' || name || ' stations'
  ,'all'
  ,tag
  ,name
  ,name || ' stations'
FROM source;

CREATE TABLE IF NOT EXISTS phenomenon (
    id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL,
    tag VARCHAR(255),
    units VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS observed_property (
    id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL,
    source_id INTEGER NOT NULL REFERENCES source(id),
    phenomenon_id INTEGER NOT NULL REFERENCES phenomenon(id),
    foreign_tag VARCHAR(255),
    foreign_units VARCHAR(255),
    depth DECIMAL
);

CREATE TABLE IF NOT EXISTS station (
    id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL,
    source_id INTEGER NOT NULL REFERENCES source(id),    
    name VARCHAR(255),
    latitude DECIMAL,
    longitude DECIMAL,
    foreign_tag VARCHAR(255),
    tag VARCHAR(255),
    description VARCHAR(255),
    platform_type VARCHAR(255),
    active BOOLEAN DEFAULT true NOT NULL,
    time_begin TIMESTAMP,
    time_end TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sensor (
    id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL,
    station_id INTEGER NOT NULL REFERENCES station(id),
    tag VARCHAR(255),
    description VARCHAR(255),
    active BOOLEAN DEFAULT true NOT NULL
);

CREATE TABLE IF NOT EXISTS network_source (
    network_id INTEGER NOT NULL REFERENCES network(id),
    source_id INTEGER NOT NULL REFERENCES source(id),
    PRIMARY KEY(network_id, source_id)
) AS 
SELECT a.id as network_id, b.id as source_id
FROM network AS a
JOIN source AS b
 ON a.source_tag = b.tag
WHERE a.tag = 'all';

CREATE TABLE IF NOT EXISTS network_station (
    network_id INTEGER NOT NULL REFERENCES network(id),
    station_id INTEGER NOT NULL REFERENCES station(id),
    PRIMARY KEY(network_id, station_id)
);

CREATE TABLE IF NOT EXISTS sensor_phenomenon (
    sensor_id INTEGER NOT NULL REFERENCES sensor(id),
    phenomenon_id INTEGER NOT NULL REFERENCES phenomenon(id),
    PRIMARY KEY(sensor_id, phenomenon_id)
);
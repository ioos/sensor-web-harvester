FROM maven:3
MAINTAINER Shane St Clair<shane@axds.co>

WORKDIR /usr/local/src

ADD pom.xml /usr/local/src/pom.xml
ADD sensor-web-harvester-app/pom.xml /usr/local/src/sensor-web-harvester-app/pom.xml
ADD sensor-web-harvester-iso/pom.xml /usr/local/src/sensor-web-harvester-iso/pom.xml
ADD sensor-web-harvester-main/pom.xml /usr/local/src/sensor-web-harvester-main/pom.xml
ADD sensor-web-harvester-source/pom.xml /usr/local/src/sensor-web-harvester-source/pom.xml
RUN mvn clean verify --fail-never

#download scala deps
RUN mvn scala:help

ADD . /usr/local/src

RUN mvn clean package \
    && mkdir -p /srv/sensor-web-harvester \
    && mv sensor-web-harvester-app/target/sensor-web-harvester-*.jar /srv/sensor-web-harvester/sensor-web-harvester.jar \
    && rm -rf /usr/local/src/*

WORKDIR /srv/sensor-web-harvester

#Add sensor user
RUN useradd --system --home-dir=/srv/sensor-web-harvester sensor \
      && chown -R sensor:sensor /srv/sensor-web-harvester

#Run as sensor user
USER sensor

CMD java -jar /srv/sensor-web-harvester/sensor-web-harvester.jar

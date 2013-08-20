/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.harvester.iso

import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.data.LocalStation
import java.io.File
import java.util.Calendar
import org.apache.log4j.Logger

trait ISOWriter {
  def writeISOFile(station: LocalStation)
}

case class ServiceIdentification(val srvAbstract: String,
                                 val serviceType: String,
                                 val id: String,
                                 val citation: ServiceIdentificationCitation,
                                 val extent: ServiceIdentificationExtent,
                                 val ops: List[ServiceIdentificationOperations])
case class ServiceIdentificationCitation(val title: String,
                                         val organizationName: String,
                                         val role: String)
case class ServiceIdentificationExtent(val lat: String,
                                       val lon: String,
                                       val timeBegin: String,
                                       val timeEnd: String,
                                       val vertMin: String = "0",
                                       val vertMax: String = "0")
case class ServiceIdentificationOperations(val opName: String,
                                           val cpUrl: String,
                                           val cpName: String,
                                           val cpDesc: String,
                                           val cpFunction: String = "download")

case class DataIdentification(val idAbstract: String,
                              val citation: DataIdentificationCitation,
                              val keywords: List[String],
                              val aggregate: DataIdentificationAggregate,
                              val extent: ServiceIdentificationExtent)
                              
case class DataIdentificationCitation(val title: String)

case class DataIdentificationAggregate(val title: String,
                                       val code: String)


case class Contact(val indivName: String,
                   val orgName: String,
                   val phone: String,
                   val address: String,
                   val city: String,
                   val state: String,
                   val postal: String,
                   val email: String,
                   val webId: String,
                   val webAddress: String,
                   val role: String = "pointOfContact")

case class Dimension(val name: String,
                     val descript: String)

class ISOWriterImpl(private val stationQuery: StationQuery,
    private val templateFile: String,
    private val isoWriteDirectory: String,
    private val overwrite: Boolean) extends ISOWriter {

  private val currentDate = Calendar.getInstance
  private val LOGGER = Logger.getLogger(getClass())
  private var lstation: LocalStation = null
  private var serviceIdentification: List[ServiceIdentification] = Nil
  private var contacts: List[Contact] = Nil
  private var fileIdentifier: String = ""
  private var dataIdentification: DataIdentification = null
  private var dimensions: List[Dimension] = Nil
  
  def writeISOFile(station: LocalStation) {
    lstation = station
    val file = new java.io.File(templateFile)
    
    val source = station.getSource
    // check to see if a directory with the source name exists
    val sourceDir = new File(isoWriteDirectory + "/" + source.getName.toLowerCase)
    if (!sourceDir.exists) {
      if (!sourceDir.mkdir)
        if (!sourceDir.mkdirs)
          LOGGER error "Could not make directory " + sourceDir.getAbsolutePath + " !!!"
    }
    
    if (!overwrite) {
      // get a file list and see if the current station already exists; skip if it does
      val files = sourceDir.listFiles
      for (tfile <- files) {
        if (tfile.getName.toLowerCase.contains(getForeignTag(station))) {
          LOGGER info "Skipping: " + getForeignTag(station)
          return
        }
      }
    }
    
    if (!initialSetup(station)) {
      LOGGER error "Could not setup for iso writer"
      return
    }
    
    val fileName = sourceDir.getAbsolutePath + "/" + getForeignTag(station) + ".xml"
    serviceIdentification = getServiceInformation(station)
    contacts = getContacts(station)
    fileIdentifier = getFileIdentifier(station)
    dataIdentification = getDataIdentification(station)
    dimensions = getSensorTagsAndNames(station).map(d => new Dimension(d._2,d._1))
    val outNode = writeISOOutput
    try {
      val doc = new ElemExtras(outNode).toJdkDoc
      val file = new java.io.File(fileName)
      file.createNewFile
      val writer = new java.io.FileWriter(file)
      XmlHelpers.writeXMLPrettyPrint(doc, writer)
      LOGGER info "wrote iso file to " + file.getPath
      writer.flush
      writer.close
    } catch {
      case ex: Exception => {
          LOGGER error ex.toString
          ex.printStackTrace()
      }
    }
  }
  
  // The below should be overwritten in the subclasses /////////////////////////
  protected def initialSetup(station: LocalStation) : Boolean = { true }
  
  protected def getSensorTagsAndNames(station: LocalStation) : List[(String,String)] = {
    val retval = for (i <- 0 until station.getSensors.size) yield {
      val slist = for (j <- 0 until station.getSensors.get(i).getPhenomena.size) yield {
        val tag = station.getSensors.get(i).getPhenomena.get(j).getId
        val name = station.getSensors.get(i).getPhenomena.get(j).getName
        (tag,name)
      }
      slist.toList
    }
    retval.toList.flatten
  }
  
  protected def getServiceInformation(station: LocalStation) : List[ServiceIdentification] = {Nil}
  
  protected def getContacts(station: LocalStation) : List[Contact] = Nil

  protected def getExtent(station: LocalStation) : ServiceIdentificationExtent = {null}
  
  protected def getFileIdentifier(station: LocalStation) : String = { station.databaseStation.foreign_tag.toLowerCase }
  
  protected def getDataIdentification(station: LocalStation) : DataIdentification = {
    val idabstract = station.databaseStation.description match {
      case "" => station.databaseStation.name
      case _ => station.databaseStation.description
    }
    val citation = new DataIdentificationCitation(station.getLongName)
    val keywords = getSensorTagsAndNames(station).map(_._2)
    val agg = null
    val extent = getExtent(station)

    new DataIdentification(idabstract,citation,keywords,agg,extent)
  }
  
  protected def getForeignTag(station: LocalStation) : String = { station.databaseStation.foreign_tag.toLowerCase.replace("wmo:", "") }
  
  //////////////////////////////////////////////////////////////////////////////
  
  private def readInTemplate() : Option[scala.xml.Elem] = {
    try {
      Some(scala.xml.XML.loadFile(new File(templateFile)))
    } catch{
      case ex: Exception => {
          LOGGER error "Unable to load file into xml: " + templateFile + "\n" + ex.toString
          None
      }
    }
  }
  
  private def addDimension(node:scala.xml.Elem, tag: String, desc: String) : scala.xml.Elem = {
    val newChild = <gmd:dimension>
        <gmd:MD_Band>
          <gmd:sequenceIdentifier>
            <gco:MemberName>
              <gco:aName>
                <gco:CharacterString>{tag}</gco:CharacterString>
              </gco:aName>
            </gco:MemberName>
          </gmd:sequenceIdentifier>
          <gmd:descriptor>
            <gco:CharacterString>{desc}</gco:CharacterString>
          </gmd:descriptor>
        </gmd:MD_Band>
      </gmd:dimension>
    scala.xml.Elem(node.prefix, node.label, node.attributes, node.scope, node.child ++ newChild : _*)
  }
  
  private def addGLOSResponsibleParty() : scala.xml.Elem = {
    val newChild = <gmd:CI_ResponsibleParty>
              <gmd:individualName>
                <gco:CharacterString>GLOS DMAC</gco:CharacterString>
              </gmd:individualName>
              <gmd:organisationName>
                <gco:CharacterString>Great Lakes Observing System</gco:CharacterString>
              </gmd:organisationName>
              <gmd:contactInfo>
                <gmd:CI_Contact>
                  <gmd:phone>
                    <gmd:CI_Telephone>
                      <gmd:voice>
                        <gco:CharacterString>(734) 332-6113</gco:CharacterString>
                      </gmd:voice>
                    </gmd:CI_Telephone>
                  </gmd:phone>
                  <gmd:address>
                    <gmd:CI_Address>
                      <gmd:deliveryPoint>
                        <gco:CharacterString>229 Nickels Arcade</gco:CharacterString>
                      </gmd:deliveryPoint>
                      <gmd:city>
                        <gco:CharacterString>Ann Arbor</gco:CharacterString>
                      </gmd:city>
                      <gmd:administrativeArea>
                        <gco:CharacterString>Michigan</gco:CharacterString>
                      </gmd:administrativeArea>
                      <gmd:postalCode>
                        <gco:CharacterString>48104</gco:CharacterString>
                      </gmd:postalCode>
                      <gmd:electronicMailAddress>
                        <gco:CharacterString>dmac@glos.us</gco:CharacterString>
                      </gmd:electronicMailAddress>
                    </gmd:CI_Address>
                  </gmd:address>
                  <gmd:onlineResource>
                    <gmd:CI_OnlineResource id={"glos_url"}>
                    <gmd:linkage>
                      <gmd:URL>http://glos.us</gmd:URL>
                    </gmd:linkage>
                  </gmd:CI_OnlineResource>
                </gmd:onlineResource>
                </gmd:CI_Contact>
              </gmd:contactInfo>
              <gmd:role>
                <gmd:CI_RoleCode codeList="http://www.ngdc.noaa.gov/metadata/published/xsd/schema/resources/CodeList/gmxCodelists.xml#CI_RoleCode" codeListValue="pointOfContact">pointOfContact</gmd:CI_RoleCode>
              </gmd:role>
            </gmd:CI_ResponsibleParty>

    newChild
  }
  
  protected def formatDateTime(datetime: Calendar) : String = datetime.get(Calendar.YEAR) + "-" +
    (if(datetime.get(Calendar.MONTH)+1 < 10) "0" else "") + (datetime.get(Calendar.MONTH)+1) + "-" +
      (if(datetime.get(Calendar.DAY_OF_MONTH) < 10) "0" else "") + datetime.get(Calendar.DAY_OF_MONTH) + "T" +
      (if(datetime.get(Calendar.HOUR_OF_DAY) < 10) "0" else "") +datetime.get(Calendar.HOUR_OF_DAY) + ":" +
      (if(datetime.get(Calendar.MINUTE) < 10) "0" else "") + datetime.get(Calendar.MINUTE) + ":" +
      (if(datetime.get(Calendar.SECOND) < 10) "0" else "") + datetime.get(Calendar.SECOND) + "." +
      (if(datetime.get(Calendar.MILLISECOND) < 100) (if(datetime.get(Calendar.MILLISECOND) < 10) "00" else "0") else "") +
      datetime.get(Calendar.MILLISECOND) + "Z"
  
  protected def formatDate(date: Calendar) : String = date.get(Calendar.YEAR) + "-" +
    (if(date.get(Calendar.MONTH)+1 < 10) "0" else "") + (date.get(Calendar.MONTH) + 1) + "-" +
    (if(date.get(Calendar.DAY_OF_MONTH) < 10) "0" else "") + date.get(Calendar.DAY_OF_MONTH)

  private def writeISOOutput() : scala.xml.Elem = {
    //         <gmd:contact>{addGLOSResponsibleParty}</gmd:contact>
    val retval =
      <gmi:MI_Metadata xmlns:gmi="http://www.isotc211.org/2005/gmi" xmlns:srv="http://www.isotc211.org/2005/srv"
        xmlns:gmx="http://www.isotc211.org/2005/gmx" xmlns:gsr="http://www.isotc211.org/2005/gsr"
        xmlns:gss="http://www.isotc211.org/2005/gss" xmlns:xs="http://www.w3.org/2001/XMLSchema"
        xmlns:gts="http://www.isotc211.org/2005/gts" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:gml="http://www.opengis.net/gml/3.2" xmlns:xlink="http://www.w3.org/1999/xlink"
        xmlns:gco="http://www.isotc211.org/2005/gco" xmlns:gmd="http://www.isotc211.org/2005/gmd"
        xsi:schemaLocation="http://www.isotc211.org/2005/gmi http://www.ngdc.noaa.gov/metadata/published/xsd/schema.xsd">
        <gmd:fileIdentifier>{addFileIdentifier(fileIdentifier)}</gmd:fileIdentifier>
        {
          for (ct <- contacts) yield {
            <gmd:contact>{addResponsibleParty(ct)}</gmd:contact>
          }
        }
        <gmd:dateStamp>{addDateStamp}</gmd:dateStamp>
        <gmd:metadataStandardName>{addMetadataStandardName}</gmd:metadataStandardName>
        <gmd:metadataStandardVersion>{addMetadataStandardVersion}</gmd:metadataStandardVersion>
        {
          if (dataIdentification != null)
            addDataIdentification(dataIdentification)
        }
        { for(srv <- serviceIdentification if srv.ops.length > 0) yield {
            addServiceIdentificationInfo(srv)
          } }
        <gmd:contentInfo>{addContentInfo}</gmd:contentInfo>
      </gmi:MI_Metadata>
    
    return retval
  }
  
  private def addFileIdentifier(fileIdent: String) : scala.xml.Elem = {
    <gco:CharacterString>{fileIdent}</gco:CharacterString>
  }
  
  private def addDateStamp() : scala.xml.Elem = {
    <gco:Date>{formatDate(Calendar.getInstance)}</gco:Date>
  }
  
  private def addMetadataStandardName() : scala.xml.Elem = {
    <gco:CharacterString>ISO 19115-2 Geographic Information - Metadata Part 2 Extensions for imagery and gridded data</gco:CharacterString>
  }
  
  private def addMetadataStandardVersion() : scala.xml.Elem = {
    <gco:CharacterString>ISO 19115-2:2009(E)</gco:CharacterString>
  }
  
  private def addDataIdentification(dI: DataIdentification) : scala.xml.Elem = {
    <gmd:identificationInfo>
      <gmd:MD_DataIdentification>
        { addDataCitation(dI.citation) }
        {
          if (dI.idAbstract != null && dI.idAbstract != "")
            <gmd:abstract>
              <gco:CharacterString>{dI.idAbstract}</gco:CharacterString>
            </gmd:abstract>
          else
            <gmd:abstract gco:nilReason="missing" />
        }
        {
          if (dI.keywords.nonEmpty)
            <gmd:descriptiveKeywords>
              <gmd:MD_Keywords>
                {dI.keywords.map(addKeyword(_))}
              </gmd:MD_Keywords>
            </gmd:descriptiveKeywords>
        }
        <gmd:language>
          <gco:CharacterString>eng</gco:CharacterString>
        </gmd:language>
        <gmd:topicCategory>
          <gmd:MD_TopicCategoryCode>inlandWaters</gmd:MD_TopicCategoryCode>
        </gmd:topicCategory>
        <gmd:extent>{addServiceExtent(dI.extent)}</gmd:extent>
      </gmd:MD_DataIdentification>
    </gmd:identificationInfo>
  }
  
  private def addKeyword(kywrd: String) : scala.xml.Elem = {
    <gmd:keyword>
      <gco:CharacterString>{kywrd}</gco:CharacterString>
    </gmd:keyword>
  }
  
  private def addDataCitation(citation: DataIdentificationCitation) = {
    if (citation != null) {
      <gmd:citation>
        <gmd:CI_Citation>
          <gmd:title>
            <gco:CharacterString>{citation.title}</gco:CharacterString>
          </gmd:title>
          <gmd:date>
            <gmd:CI_Date>
              <gmd:date>
                <gco:DateTime>{formatDateTime(Calendar.getInstance)}</gco:DateTime>
              </gmd:date>
              <gmd:dateType gco:nilReason="unknown" />
            </gmd:CI_Date>
          </gmd:date>
        </gmd:CI_Citation>
      </gmd:citation>
    } else {
      <gmd:citation gco:nilReason="missing" />
    }
  }
  
  private def addResponsibleParty(contact: Contact) : scala.xml.Elem = {
    <gmd:CI_ResponsibleParty>
      { if (contact.indivName != null && contact.indivName != "")
          <gmd:individualName>
            <gco:CharacterString>{contact.indivName}</gco:CharacterString>
          </gmd:individualName>
         else
           <gmd:individualName gco:nilReason="missing" />
      }
      { if (contact.orgName != null && contact.orgName != "")
          <gmd:organisationName>
            <gco:CharacterString>{contact.orgName}</gco:CharacterString>
          </gmd:organisationName>
         else
           <gmd:organisationName gco:nilReason="missing" />
      }
      <gmd:contactInfo>
        <gmd:CI_Contact>
          { if (contact.phone != null && contact.phone != "")
              <gmd:phone>
                  <gmd:CI_Telephone>
                    <gmd:voice>
                      <gco:CharacterString>{contact.phone}</gco:CharacterString>
                    </gmd:voice>
                  </gmd:CI_Telephone>
              </gmd:phone>
            else
              <gmd:phone gco:nilReason="missing" />
          }
          <gmd:address>
            <gmd:CI_Address>
            { if (contact.address != null && contact.address != "")
                <gmd:deliveryPoint>
                    <gco:CharacterString>{contact.address}</gco:CharacterString>
                </gmd:deliveryPoint>
              else
                <gmd:deliveryPoint gco:nilReason="missing" />
            }
            { if (contact.city != null && contact.city != "")
                <gmd:city>
                  <gco:CharacterString>{contact.city}</gco:CharacterString>
                </gmd:city>
              else
                <gmd:city gco:nilReason="missing" />
            }
            { if (contact.state != null && contact.state != "")
                <gmd:administrativeArea>
                  <gco:CharacterString>{contact.state}</gco:CharacterString>
                </gmd:administrativeArea>
              else
                <gmd:administrativeArea gco:nilReason="missing" />
            }
            { if (contact.postal != null && contact.postal != "")
                <gmd:postalCode>
                  <gco:CharacterString>{contact.postal}</gco:CharacterString>
                </gmd:postalCode>
              else
                <gmd:postalCode gco:nilReason="missing" />
            }
            { if (contact.email != null && contact.email != "")
                <gmd:electronicMailAddress>
                  <gco:CharacterString>{contact.email}</gco:CharacterString>
                </gmd:electronicMailAddress>
              else
                <gmd:electronicMailAddress gco:nilReason="missing" />
            }
            </gmd:CI_Address>
          </gmd:address>
          {
            if (contact.webAddress != null && contact.webId != null && contact.webAddress != "")
              <gmd:onlineResource>
                <gmd:CI_OnlineResource id={contact.webId + "_url"}>
                  <gmd:linkage>
                    <gmd:URL>{contact.webAddress}</gmd:URL>
                  </gmd:linkage>
                </gmd:CI_OnlineResource>
              </gmd:onlineResource>
            else
              <gmd:onlineResource gco:nilReason="missing" />
          }
        </gmd:CI_Contact>
      </gmd:contactInfo>
      { if (contact.role != null && contact.role != "")
          <gmd:role>
            <gmd:CI_RoleCode codeList="http://www.ngdc.noaa.gov/metadata/published/xsd/schema/resources/Codelist/gmxCodelists.xml#CI_RoleCode" codeListValue={contact.role}>{contact.role}</gmd:CI_RoleCode>
          </gmd:role>
         else
           <gmd:role gco:nilReason="missing" />
      }
    </gmd:CI_ResponsibleParty>
  }
  
  private def addServiceIdentificationInfo(info: ServiceIdentification) : scala.xml.Elem = {
    val II = <gmd:identificationInfo>
      <srv:SV_ServiceIdentification id={info.id}>
        <gmd:citation>
          { addServiceCitation(info.citation) }
        </gmd:citation>
        <gmd:abstract>
          <gco:CharacterString>{ info.srvAbstract }</gco:CharacterString>
        </gmd:abstract>
        <srv:serviceType>
            <gco:LocalName>{ info.serviceType }</gco:LocalName>
        </srv:serviceType>
        <srv:extent>
          { addServiceExtent(info.extent) }
        </srv:extent>
        <srv:couplingType>
          <srv:SV_CouplingType codeList="http://www.tc211.org/ISO19139/resources/codeList.xml#SV_CouplingType" codeListValue="tight">tight</srv:SV_CouplingType>
        </srv:couplingType>
        {for (op <- info.ops) yield {
            <srv:containsOperations>{addServiceOperations(op)}</srv:containsOperations>
          }}
      </srv:SV_ServiceIdentification>
    </gmd:identificationInfo>

    return II
  }
  
  private def addServiceCitation(citation: ServiceIdentificationCitation) : scala.xml.Elem = {
    val retval = 
      <gmd:CI_Citation>
        <gmd:title>
          <gco:CharacterString>{citation.title}</gco:CharacterString>
        </gmd:title>
        <gmd:date>
          <gmd:CI_Date>
             <gmd:date gco:nilReason="unknown" />
             <gmd:dateType gco:nilReason="unknown" />
          </gmd:CI_Date>
        </gmd:date>
        <gmd:citedResponsibleParty>
          <gmd:CI_ResponsibleParty>
            <gmd:individualName gco:nilReason="missing"/>
            <gmd:organisationName>
              <gco:CharacterString>{citation.organizationName}</gco:CharacterString>
            </gmd:organisationName>
            <gmd:contactInfo gco:nilReason="missing"/>
            <gmd:role>
              <gmd:CI_RoleCode codeList="http://www.ngdc.noaa.gov/metadata/published/xsd/schema/resources/CodeList/gmxCodelists.xml#CI_RoleCode" codeListValue={citation.role}>{citation.role}</gmd:CI_RoleCode>
            </gmd:role>
          </gmd:CI_ResponsibleParty>
        </gmd:citedResponsibleParty>
      </gmd:CI_Citation>
    
    return retval
  }

  private def addServiceExtent(extent: ServiceIdentificationExtent) : scala.xml.Elem = {
    val tpid = scala.util.Random.nextInt.toString
    val retval = <gmd:EX_Extent>
        <gmd:geographicElement>
          <gmd:EX_GeographicBoundingBox>
            <gmd:extentTypeCode>
              <gco:Boolean>1</gco:Boolean>
            </gmd:extentTypeCode>
            <gmd:westBoundLongitude>
              <gco:Decimal>{ extent.lon }</gco:Decimal>
            </gmd:westBoundLongitude>
            <gmd:eastBoundLongitude>
              <gco:Decimal>{ extent.lon }</gco:Decimal>
            </gmd:eastBoundLongitude>
            <gmd:southBoundLatitude>
              <gco:Decimal>{ extent.lat }</gco:Decimal>
            </gmd:southBoundLatitude>
            <gmd:northBoundLatitude>
              <gco:Decimal>{ extent.lat }</gco:Decimal>
            </gmd:northBoundLatitude>
          </gmd:EX_GeographicBoundingBox>
        </gmd:geographicElement>
        <gmd:temporalElement>
          <gmd:EX_TemporalExtent>
            <gmd:extent>
              <gml:TimePeriod id="TP-EX1">
                { if (extent.timeBegin != null && extent.timeBegin != "") {
                    if (extent.timeBegin.equals("unknown"))
                      <gml:beginPosition indeterminatePosition="unknown"/>
                    else
                      <gml:beginPosition>{extent.timeBegin}</gml:beginPosition>
                  }
                  else {
                    <gml:beginPosition indeterminatePosition="now"/>
                  }
                }
                { if (extent.timeEnd != null && extent.timeEnd != "") {
                    if (extent.timeEnd.equals("unknown"))
                        <gml:endPosition indeterminatePosition="unknown"/>
                    else
                      <gml:endPosition>{extent.timeEnd}</gml:endPosition>
                  }
                  else {
                    <gml:endPosition indeterminatePosition="now"/> 
                  }
                }
              </gml:TimePeriod>
            </gmd:extent>
          </gmd:EX_TemporalExtent>
        </gmd:temporalElement>
        <gmd:verticalElement>
          <gmd:EX_VerticalExtent>
            <gmd:minimumValue>
              <gco:Real>{ extent.vertMin }</gco:Real>
            </gmd:minimumValue>
            <gmd:maximumValue>
              <gco:Real>{ extent.vertMax }</gco:Real>
            </gmd:maximumValue>
            <gmd:verticalCRS gco:nilReason="missing" />
          </gmd:EX_VerticalExtent>
        </gmd:verticalElement>
      </gmd:EX_Extent>

    return retval
  }
  
  private def addServiceOperations(ops: ServiceIdentificationOperations) : scala.xml.Elem = {
    val retval =
      <srv:SV_OperationMetadata>
        <srv:operationName>
          <gco:CharacterString>{ ops.opName }</gco:CharacterString>
        </srv:operationName>
        <srv:DCP gco:nilReason="unknown" />
        <srv:connectPoint>
          <gmd:CI_OnlineResource>
            <gmd:linkage>
              <gmd:URL>{ ops.cpUrl }</gmd:URL>
            </gmd:linkage>
            <gmd:name>
              <gco:CharacterString>{ ops.cpName }</gco:CharacterString>
            </gmd:name>
            <gmd:description>
              <gco:CharacterString>{ ops.cpDesc }</gco:CharacterString>
            </gmd:description>
            <gmd:function>
              <gmd:CI_OnLineFunctionCode codeList="http://www.ngdc.noaa.gov/metadata/published/xsd/schema/resources/Codelist/gmxCodelists.xml#CI_OnLineFunctionCode" codeListValue={ ops.cpFunction }>{ ops.cpFunction }</gmd:CI_OnLineFunctionCode>
            </gmd:function>
          </gmd:CI_OnlineResource>
        </srv:connectPoint>
      </srv:SV_OperationMetadata>
      
    return retval
  }
  
  private def addContentInfo() : scala.xml.Elem = {
    <gmi:MI_CoverageDescription>
      <gmd:attributeDescription gco:nilReason="unknown"></gmd:attributeDescription>
      <gmd:contentType gco:nilReason="unknown"></gmd:contentType>
      {
        for (dim <- dimensions) yield {
          <gmd:dimension>
            <gmd:MD_Band>
              <gmd:sequenceIdentifier>
                <gco:MemberName>
                  <gco:aName>
                    <gco:CharacterString>{dim.name}</gco:CharacterString>
                  </gco:aName>
                  <gco:attributeType gco:nilReason="unknown" />
                </gco:MemberName>
              </gmd:sequenceIdentifier>
              <gmd:descriptor>
                <gco:CharacterString>{dim.descript}</gco:CharacterString>
              </gmd:descriptor>
            </gmd:MD_Band>
          </gmd:dimension>
        }
      }
    </gmi:MI_CoverageDescription>
  }
  
  private def fileListHtml(files: List[String]) : scala.xml.Elem = {
    <html>
      <body>
        <p>File List:</p>
        <ul>
          {for (file <- files) yield {
              <li>
                { file }
              </li>
            }
          }
        </ul>
      </body>
    </html>
  }
}
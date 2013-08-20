/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.harvester.iso

import java.io.Writer
import javax.xml.parsers.DocumentBuilderFactory
import org.apache.log4j.Logger
import org.w3c.dom.bootstrap.DOMImplementationRegistry
import org.w3c.dom.ls.DOMImplementationLS
import scala.xml.Atom
import scala.xml.Comment
import scala.xml.Elem
import scala.xml.Node
import scala.xml.Text
import scala.xml.TopScope

object XmlHelpers {
  val docBuilder =
    javax.xml.parsers.DocumentBuilderFactory.newInstance().newDocumentBuilder()
  
  def writeXMLPrettyPrint(doc: org.w3c.dom.Document, writer: Writer) {
    val registry = DOMImplementationRegistry.newInstance()
    val impl = registry.getDOMImplementation("LS").asInstanceOf[DOMImplementationLS]
    val serializer = impl.createLSSerializer()
    val xmlOut = impl.createLSOutput
    serializer.getDomConfig.setParameter(("format-pretty-print"), true)
    xmlOut.setCharacterStream(writer)
    serializer.write(doc, xmlOut)
  }
}

class NodeExtras(n: Node) {
  
  private val LOGGER = Logger.getLogger(getClass())
  private var writeNS: Boolean = true
  
  protected def toJdkNode(node: Node, doc: org.w3c.dom.Document): org.w3c.dom.Node = {
      node match {
        case elem: Elem =>
          // XXX: ns
          val r = {
            doc.createElement(node.prefix + ":" + node.label)
          }
          if (writeNS) {
            writeNS = false
            writeScope(r, elem.scope)
          }
          for (a <- elem.attributes) {
            r.setAttribute(a.prefixedKey, a.value.text)
          }
          for (c <- elem.child) {
            r.appendChild(toJdkNode(c,doc))
          }
          r
        case text: Text if (!node.label.contains("#")) => {
            val nn = doc.createElementNS(node.namespace, node.label)
            nn.setTextContent(text.data)
            nn
          }
        case Text(text) => doc.createTextNode(text)
        case Comment(comment) => doc.createComment(comment)
        // not sure
        case a: Atom[_] if (!node.label.contains("#")) => {
            val nn = doc.createElementNS(node.namespace, node.label)
            nn.setTextContent(a.data.toString)
            nn
          }
        case a: Atom[_] => doc.createTextNode(a.data.toString)
        //// XXX: other types
        //case x => throw new Exception(x.getClass.getName)
    }
  }
  
  private def writeScope(elem: org.w3c.dom.Element, ns:scala.xml.NamespaceBinding) : Unit = {
    if (!ns.equals(TopScope)) {
      elem.setAttribute("xmlns:" + ns.prefix, ns.uri)
      writeScope(elem,ns.parent)
    }
  }
  
  private def checkScope(ns: scala.xml.NamespaceBinding) : Unit = {
    LOGGER info "namespace: " + ns.prefix + " = " + ns.uri
    if (!ns.equals(TopScope))
      checkScope(ns.parent)
  }
}

class ElemExtras(e: Elem) extends NodeExtras(e) {
//  override def toJdkNode(node:Node, doc: org.w3c.dom.Document) =
//    super.toJdkNode(node, doc).asInstanceOf[org.w3c.dom.Element]
  
  def toJdkDoc = {
    val doc = XmlHelpers.docBuilder.getDOMImplementation.createDocument(
        null, null, null)
    doc.appendChild(toJdkNode(e,doc))
    doc
  }
}

//implicit def nodeExtras(n: Node) = new NodeExtras(n)
//implicit def elemExtras(e: Elem) = new ElemExtras(e)

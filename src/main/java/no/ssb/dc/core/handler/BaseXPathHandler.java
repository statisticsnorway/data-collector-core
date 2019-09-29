package no.ssb.dc.core.handler;

import no.ssb.dc.api.Interfaces;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.error.ConversionException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;

import static org.w3c.dom.Node.ATTRIBUTE_NODE;
import static org.w3c.dom.Node.CDATA_SECTION_NODE;
import static org.w3c.dom.Node.DOCUMENT_FRAGMENT_NODE;
import static org.w3c.dom.Node.DOCUMENT_NODE;
import static org.w3c.dom.Node.DOCUMENT_TYPE_NODE;
import static org.w3c.dom.Node.ELEMENT_NODE;
import static org.w3c.dom.Node.ENTITY_NODE;
import static org.w3c.dom.Node.ENTITY_REFERENCE_NODE;
import static org.w3c.dom.Node.NOTATION_NODE;
import static org.w3c.dom.Node.PROCESSING_INSTRUCTION_NODE;
import static org.w3c.dom.Node.TEXT_NODE;

abstract class BaseXPathHandler extends AbstractHandler<Interfaces.XPath> {
    BaseXPathHandler(Interfaces.XPath node) {
        super(node);
    }

    static XMLReader createSAXFactory() {
        try {
            SAXParserFactory sax = SAXParserFactory.newInstance();
            sax.setNamespaceAware(false);
            return sax.newSAXParser().getXMLReader();
        } catch (SAXException | ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    static DocumentBuilder createDocumentBuilder() {
        try {
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            documentBuilderFactory.setNamespaceAware(false);
            return documentBuilderFactory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    static String serialize(Node node) {
        try {
            StringWriter writer = new StringWriter();
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.transform(new DOMSource(node), new StreamResult(writer));
            return writer.toString();

        } catch (TransformerConfigurationException e) {
            throw new RuntimeException(e);
        } catch (TransformerException e) {
            throw new RuntimeException(e);
        }
    }

    static Document deserialize(byte[] source) {
        try {
            XMLReader reader = createSAXFactory();
            SAXSource saxSource = new SAXSource(reader, new InputSource(new ByteArrayInputStream(source)));
            DocumentBuilder documentBuilder = createDocumentBuilder();
            Document doc = documentBuilder.parse(saxSource.getInputSource());
            doc.normalizeDocument();
            return doc;

        } catch (SAXException | IOException e) {
            throw new ConversionException(new String(source), e);
        }
    }

    static Document convertNodeToDocument(Node node) {
        Document doc = createDocumentBuilder().newDocument();
        Node importNode = doc.importNode(node, true);
        doc.appendChild(importNode);
        return doc;
    }

    static Position<?> convertNodeToPosition(Node node) {
        Position<?> position = null;

        switch (node.getNodeType()) {
            case ELEMENT_NODE: {
                position = new Position<>(node.getTextContent());
                break;
            }

            case ATTRIBUTE_NODE: {
                position = new Position<>(node.getNodeValue());
                break;
            }

            case TEXT_NODE: {
                position = new Position<>(node.getNodeValue());
                break;
            }

            case CDATA_SECTION_NODE:
            case ENTITY_REFERENCE_NODE:
            case ENTITY_NODE:
            case PROCESSING_INSTRUCTION_NODE:
            case DOCUMENT_NODE:
            case DOCUMENT_TYPE_NODE:
            case DOCUMENT_FRAGMENT_NODE:
            case NOTATION_NODE:
                break;

            default:
                throw new IllegalArgumentException("Node-type not supported!");
        }

        return position;
    }

}

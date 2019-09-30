package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.error.ConversionException;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.XPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.namespace.QName;
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
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static javax.xml.xpath.XPathConstants.NODE;
import static javax.xml.xpath.XPathConstants.NODESET;
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

@Handler(forClass = XPath.class)
public class XPathHandler extends AbstractQueryHandler<XPath> {

    private static final Logger LOG = LoggerFactory.getLogger(XPathHandler.class);

    private final XPathFactory xpathFactory;

    public XPathHandler(XPath node) {
        super(node);
        xpathFactory = XPathFactory.newInstance();
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

    static Document convertNodeToDocument(Node node) {
        Document doc = createDocumentBuilder().newDocument();
        Node importNode = doc.importNode(node, true);
        doc.appendChild(importNode);
        return doc;
    }

    <QUERY_RESULT, FUNCTION_RESULT> FUNCTION_RESULT evaluateXPath(Document document, QName returnType, Function<QUERY_RESULT, FUNCTION_RESULT> converter) {
        try {
            QUERY_RESULT result = (QUERY_RESULT) xpathFactory.newXPath().compile(node.expression()).evaluate(document, returnType);

            if (result == null) {
                throw new IllegalArgumentException("XPath expression " + node.expression() + " returned null for node-item-xml: " + serialize(document));
            }

            return converter.apply(result);

        } catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        return super.execute(input);
    }

    @Override
    public byte[] serialize(Object node) {
        try {
            StringWriter writer = new StringWriter();
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.transform(new DOMSource((Node) node), new StreamResult(writer));
            return writer.toString().getBytes();

        } catch (TransformerConfigurationException e) {
            throw new RuntimeException(e);
        } catch (TransformerException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object deserialize(byte[] source) {
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

    Document asDocument(Object data) {
        if (data instanceof Document) {
            return (Document) data;
        } else if (data instanceof byte[]) {
            return (Document) deserialize((byte[]) data);
        } else if (data instanceof String) {
            return (Document) deserialize(((String) data).getBytes());
        } else {
            throw new IllegalArgumentException("Param value not supported: " + data);
        }
    }

    @Override
    public List<?> queryList(Object data) {
        Document document = asDocument(data);

        Function<NodeList, List<Document>> converter = (nodeList -> {
            List<Document> values = new ArrayList<>();
            for (int i = 0; i < nodeList.getLength(); i++) {
                values.add(convertNodeToDocument(nodeList.item(i)));
            }
            return values;
        });

        return evaluateXPath(document, NODESET, converter);
    }

    @Override
    public Object queryObject(Object data) {
        Document document = asDocument(data);
        return evaluateXPath(document, NODE, XPathHandler::convertNodeToDocument);
    }

    @Override
    public String queryStringLiteral(Object data) {
        Document document = asDocument(data);

        Function<Node, String> converter = (node -> {
            switch (node.getNodeType()) {
                case ELEMENT_NODE:
                    return node.getTextContent();

                case ATTRIBUTE_NODE:
                    return node.getNodeValue();


                case TEXT_NODE:
                    return node.getNodeValue();

                case CDATA_SECTION_NODE:
                case ENTITY_REFERENCE_NODE:
                case ENTITY_NODE:
                case PROCESSING_INSTRUCTION_NODE:
                case DOCUMENT_NODE:
                case DOCUMENT_TYPE_NODE:
                case DOCUMENT_FRAGMENT_NODE:
                case NOTATION_NODE:
                default:
                    throw new IllegalArgumentException("Node-type not supported!");
            }
        });

        return evaluateXPath(document, NODE, converter);
    }


}

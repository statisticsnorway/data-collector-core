package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.XPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static javax.xml.xpath.XPathConstants.NODE;
import static javax.xml.xpath.XPathConstants.NODESET;
import static org.w3c.dom.Node.*;

@Handler(forClass = XPath.class)
public class XPathHandler extends AbstractQueryHandler<XPath> {

    private static final Logger LOG = LoggerFactory.getLogger(XPathHandler.class);

    private final XPathFactory xpathFactory;
    private final DocumentParserFeature parser;

    public XPathHandler(XPath node) {
        super(node);
        parser = Queries.parserFor(node.getClass());
        xpathFactory = XPathFactory.newInstance();
    }

    static Document convertNodeToDocument(Node node) {
        Document doc = XPathParser.createDocumentBuilder().newDocument();
        Node importNode = doc.importNode(node, true);
        doc.appendChild(importNode);
        return doc;
    }

    <QUERY_RESULT, FUNCTION_RESULT> FUNCTION_RESULT evaluateXPath(Document document, QName returnType, Function<QUERY_RESULT, FUNCTION_RESULT> converter) {
        try {
            javax.xml.xpath.XPath xpath = xpathFactory.newXPath();
            xpath.setNamespaceContext(new IgnoreNamespaceResolver(document));
            QUERY_RESULT result = (QUERY_RESULT) xpath.compile(evaluateExpression(node.expression())).evaluate(document, returnType);

            if (result == null) {
                throw new IllegalArgumentException(String.format("XPath expression %s returned null for node-item-xml:%n%s",
                        node.expression(), new String(parser.serialize(document), StandardCharsets.UTF_8)));
            }

            return converter.apply(result);

        } catch (IllegalArgumentException e) {
            throw e;
        } catch (XPathExpressionException e) {
            throw new RuntimeException(String.format("XPath expression %s returned null for node-item-xml:%n%s",
                    node.expression(), new String(parser.serialize(document), StandardCharsets.UTF_8)), e);
        }
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        return super.execute(input);
    }

    Document asDocument(Object data) {
        if (data instanceof Document) {
            return (Document) data;

        } else if (data instanceof byte[]) {
            return (Document) parser.deserialize((byte[]) data);

        } else if (data instanceof String) {
            return (Document) parser.deserialize(((String) data).getBytes());

        } else {
            throw new IllegalArgumentException("Param value not supported: " + data);
        }
    }

    @Override
    public List<?> evaluateList(Object data) {
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
    public Object evaluateObject(Object data) {
        Document document = asDocument(data);
        return evaluateXPath(document, NODE, XPathHandler::convertNodeToDocument);
    }

    @Override
    public String evaluateStringLiteral(Object data) {
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

    static public class IgnoreNamespaceResolver implements NamespaceContext {
        private Document document;

        public IgnoreNamespaceResolver(Document document) {
            this.document = document;
        }

        @Override
        public String getNamespaceURI(String prefix) {
            if (prefix.equals(XMLConstants.DEFAULT_NS_PREFIX)) {
                return document.lookupNamespaceURI(null);
            } else {
                return document.lookupNamespaceURI(prefix);
            }
        }

        @Override
        public String getPrefix(String namespaceURI) {
            return document.lookupPrefix(namespaceURI);
        }

        @Override
        public Iterator<String> getPrefixes(String namespaceURI) {
            return Collections.emptyIterator();
        }
    }
}

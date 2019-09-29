package no.ssb.dc.core.handler;

import no.ssb.dc.api.Handler;
import no.ssb.dc.api.Interfaces;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.delegate.QueryItem;
import no.ssb.dc.api.delegate.QueryItemList;
import no.ssb.dc.api.delegate.QueryItemListItem;
import no.ssb.dc.api.delegate.QueryNodeLiteral;
import no.ssb.dc.api.delegate.QueryPosition;
import no.ssb.dc.api.delegate.QueryPositionMap;
import no.ssb.dc.api.delegate.QueryType;
import no.ssb.dc.api.delegate.Tuple;
import no.ssb.dc.api.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.namespace.QName;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static javax.xml.xpath.XPathConstants.NODE;
import static javax.xml.xpath.XPathConstants.NODESET;

@Handler(forClass = Interfaces.XPath.class)
public class XPathHandler extends BaseXPathHandler {

    private static final Logger LOG = LoggerFactory.getLogger(XPathHandler.class);

    public XPathHandler(Interfaces.XPath node) {
        super(node);
    }


    /*
     * ExecutionInput.state[Constants.ITEM_LIST]     returns ExecutionOutput.state[QUERY_RESULT] as item-list:          List<Document:entry>
     * ExecutionInput.state[Constants.POSITION_MAP]  returns ExecutionOutput.state[QUERY_RESULT] as expected-positions: Map<Position<?>, String:entry-xml>
     * ExecutionInput.state[Constants.POSITION_ITEM] returns ExecutionOutput.state[QUERY_RESULT] as position-item:      Tuple<Position<?>, String:entry-xml>
     */
    @Override
    public ExecutionContext execute(ExecutionContext input) {
        ExecutionContext output = ExecutionContext.empty();

        QueryType queryType = input.state(QueryType.class);

        if (queryType == null) {
            throw new IllegalStateException("Constants.QUERY_TYPE is NOT set in state()");
        }

        if (queryType == QueryType.ITEM_LIST) {
            Response response = input.state(Response.class);
            byte[] xmlBytes = response.body();
            QueryItemList queryItemList = new XPathItemList(xmlBytes, node.expression());
            output.state(QueryStateHolder.QUERY_RESULT, queryItemList.list());

        } else if (queryType == QueryType.ITEM_LIST_ITEM) {
            Document document = input.state(QueryStateHolder.QUERY_DATA);
            QueryItemListItem queryItem = new XPathItemListItem(document, node.expression());
            Tuple<Position<?>, String> tuple = queryItem.item();
            output.state(QueryStateHolder.QUERY_RESULT, tuple);

        } else if (queryType == QueryType.POSITION_MAP) {
            List<?> itemList = input.state(QueryStateHolder.QUERY_DATA);
            QueryPositionMap queryPositionMap = new XPathPositionMap(itemList, node.expression());
            output.state(QueryStateHolder.QUERY_RESULT, queryPositionMap.map());

        } else if (queryType == QueryType.POSITION_ITEM) {
            String xml = input.state(QueryStateHolder.QUERY_DATA);
            QueryPosition queryItem = new XPathPosition(xml, node.expression());
            Tuple<Position<?>, String> tuple = queryItem.item();
            output.state(QueryStateHolder.QUERY_RESULT, tuple);

        } else if (queryType == QueryType.ITEM_NODE_LITERAL) {
            Document document = input.state(QueryStateHolder.QUERY_DATA);
            QueryNodeLiteral queryItem = new XPathNodeLiteral(document, node.expression());
            Tuple<String, String> tuple = queryItem.item();
            output.state(QueryStateHolder.QUERY_RESULT, tuple);

        } else if (queryType == QueryType.ITEM_STRING) {
            String xml = input.state(QueryStateHolder.QUERY_DATA);
            QueryItem queryItem = new XPathItem(xml, node.expression());
            Tuple<String, String> tuple = queryItem.item();
            output.state(QueryStateHolder.QUERY_RESULT, tuple);

        } else {
            throw new IllegalStateException("QueryType : " + queryType + " is not implemented!");
        }

        return output;
    }


    static class XPathItemList implements QueryItemList {

        private final byte[] xml;
        private final String expression;

        public XPathItemList(byte[] xml, String expression) {
            this.xml = xml;
            this.expression = expression;
        }

        private List<Document> convert(NodeList nodeList) {
            List<Document> values = new ArrayList<>();

            for (int i = 0; i < nodeList.getLength(); i++) {
                values.add(convertNodeToDocument(nodeList.item(i)));
            }

            return values;
        }

        @Override
        public List<?> list() {
            XPathExpression<NodeList, List<Document>> xpathExpression = new XPathExpression<>(xml, expression, NODESET, XPathFactory.newInstance());
            return xpathExpression.evaluate(this::convert);
        }
    }

    static class XPathItemListItem implements QueryItemListItem {

        private final Document document;
        private final String expression;

        public XPathItemListItem(Document document, String expression) {
            this.document = document;
            this.expression = expression;
        }

        Tuple<Position<?>, String> convert(Node node) {
            Position<?> position = convertNodeToPosition(node);

            if (position == null) {
                throw new RuntimeException("Non-handled node-type: " + node.getNodeType());
            }

            return new Tuple<>(position, serialize(document));
        }


        @Override
        public Tuple<Position<?>, String> item() {
            XPathExpression<Node, Tuple<Position<?>, String>> xpathExpression = new XPathExpression<>(document, expression, NODE, XPathFactory.newInstance());
            return xpathExpression.evaluate(this::convert);
        }
    }

    static class XPathPositionMap implements QueryPositionMap {

        private final List<Document> itemList;
        private final String expression;

        public XPathPositionMap(List<?> itemList, String expression) {
            this.itemList = (List<Document>) itemList;
            this.expression = expression;
        }

        @Override
        public Map<Position<?>, ?> map() {
            Map<Position<?>, String> itemListPositionMap = new LinkedHashMap<>();

            XPathFactory xpathFactory = XPathFactory.newInstance();
            for (Document nodeDocument : itemList) {
                XPathExpression<Node, Tuple<Position<?>, String>> xpathExpression = new XPathExpression<>(nodeDocument, expression, NODE, xpathFactory);
                Tuple<Position<?>, String> tuple = xpathExpression.evaluate(node -> {
                    Position<?> position = convertNodeToPosition(node);

                    if (position == null) {
                        throw new RuntimeException("Non-handled node-type: " + node.getNodeType());
                    }

                    return new Tuple<>(position, serialize(nodeDocument));
                });
                itemListPositionMap.put(tuple.getKey(), tuple.getValue());
            }

            return itemListPositionMap;
        }
    }

    static class XPathPosition implements QueryPosition {

        private final String xml;
        private final String expression;

        public XPathPosition(String xml, String expression) {
            this.xml = xml;
            this.expression = expression;
        }

        Tuple<Position<?>, String> convert(Node node) {
            Position<?> position = convertNodeToPosition(node);

            if (position == null) {
                throw new RuntimeException("Non-handled node-type: " + node.getNodeType());
            }

            return new Tuple<>(position, xml);
        }

        @Override
        public Tuple<Position<?>, String> item() {
            Document doc = deserialize(xml.getBytes());
            XPathExpression<Node, Tuple<Position<?>, String>> xpathExpression = new XPathExpression<>(doc, expression, NODE, XPathFactory.newInstance());
            return xpathExpression.evaluate(this::convert);
        }
    }

    static class XPathNodeLiteral implements QueryNodeLiteral {

        private final Document document;
        private final String expression;

        public XPathNodeLiteral(Document document, String expression) {
            this.document = document;
            this.expression = expression;
        }

        Tuple<String, String> convert(Node node) {
            return new Tuple<>(node.getTextContent(), serialize(document));
        }

        @Override
        public Tuple<String, String> item() {
            XPathExpression<Node, Tuple<String, String>> xpathExpression = new XPathExpression<>(document, expression, NODE, XPathFactory.newInstance());
            return xpathExpression.evaluate(this::convert);
        }
    }

    static class XPathItem implements QueryItem {

        private final String xml;
        private final String expression;

        public XPathItem(String xml, String expression) {
            this.xml = xml;
            this.expression = expression;
        }

        Tuple<String, String> convert(Node node) {
            return new Tuple<>(node.getTextContent(), xml);
        }

        @Override
        public Tuple<String, String> item() {
            Document doc = deserialize(xml.getBytes());
            XPathExpression<Node, Tuple<String, String>> xpathExpression = new XPathExpression<>(doc, expression, NODE, XPathFactory.newInstance());
            return xpathExpression.evaluate(this::convert);
        }
    }

    public static class XPathExpression<T, R> {
        public final Document document;
        public final String expression;
        public final QName returnType;
        public final XPathFactory xpathFactory;

        public XPathExpression(byte[] source, String expression, QName returnType, XPathFactory xpathFactory) {
            this.document = deserialize(source);
            this.expression = expression;
            this.returnType = returnType;
            this.xpathFactory = xpathFactory;
        }

        public XPathExpression(Document document, String expression, QName returnType, XPathFactory xpathFactory) {
            this.document = document;
            this.expression = expression;
            this.returnType = returnType;
            this.xpathFactory = xpathFactory;
        }

        public R evaluate(Function<T, R> converter) {
            try {
                T result = (T) xpathFactory.newXPath().compile(expression).evaluate(document, returnType);

                if (result == null) {
                    throw new IllegalArgumentException("XPath expression " + expression + " returned null for node-item-xml: " + serialize(document));
                }

                return converter.apply(result);
            } catch (XPathExpressionException e) {
                throw new RuntimeException(e);
            }
        }
    }

}

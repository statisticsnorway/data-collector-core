package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.handler.QueryResult;
import no.ssb.dc.api.http.BodyHandler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.XmlToken;

import java.nio.file.Path;

@Handler(forClass = XmlToken.class)
public class XmlTokenHandler extends AbstractHandler<XmlToken> {

    private final DocumentParserFeature jsonParser;

    public XmlTokenHandler(XmlToken node) {
        super(node);
        jsonParser = Queries.parserFor(node.getClass());
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        Response response = context.state(Response.class);

        // todo is this used anywhere? maybe an empty EC should be returned
        BodyHandler<Path> bodyHandler = response.<Path>bodyHandler().orElseThrow();
        QueryResult<Path> queryResult = new QueryResult<>(bodyHandler.body());
        ExecutionContext output = ExecutionContext.empty().state(QueryResult.class, queryResult);
        return output;
    }


}

package no.ssb.dc.core.handler;

import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.JqPath;

import java.util.List;

@Handler(forClass = JqPath.class)
public class JqPathHandler extends AbstractQueryHandler<JqPath> {

    private final DocumentParserFeature jsonParser;
    private static final Scope rootScope = Scope.newEmptyScope();

    static {
        BuiltinFunctionLoader.getInstance().loadFunctions(Versions.JQ_1_6, rootScope);
    }

    public JqPathHandler(JqPath node) {
        super(node);
        jsonParser = Queries.parserFor(node.getClass());
    }

    @Override
    public List<?> evaluateList(Object data) {
        return null;
    }

    @Override
    public Object evaluateObject(Object data) {
        return null;
    }

    @Override
    public String evaluateStringLiteral(Object data) {
        return null;
    }
}

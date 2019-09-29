package no.ssb.dc.core.handler;

import no.ssb.dc.api.Handler;
import no.ssb.dc.api.Interfaces;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.delegate.QueryType;
import no.ssb.dc.api.delegate.Tuple;
import no.ssb.dc.core.executor.Executor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@Handler(forClass = Interfaces.RegEx.class)
public class RegExHandler extends AbstractHandler<Interfaces.RegEx> {

    static final Map<String, Pattern> patternCache = new ConcurrentHashMap<>();

    public RegExHandler(Interfaces.RegEx node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        ExecutionContext output = ExecutionContext.of(input);

        ExecutionContext queryInput = ExecutionContext.of(input);
        queryInput.state(QueryType.class, input.state(QueryType.class));
        queryInput.state(QueryStateHolder.QUERY_DATA, input.state(QueryStateHolder.QUERY_DATA));
        ExecutionContext queryOutput = Executor.execute(node.query(), queryInput);
        Tuple<String, String> queryTuple = queryOutput.state(QueryStateHolder.QUERY_RESULT);

        try {
            Pattern pattern = patternCache.computeIfAbsent(node.expression(), Pattern::compile);
            Matcher matcher = pattern.matcher(queryTuple.getKey());
            if (matcher.find()) {
                String value = matcher.group(0);
                output.state(QueryStateHolder.QUERY_RESULT, new Tuple<Position<?>, String>(new Position<>(value), input.state(QueryStateHolder.QUERY_DATA)));
            }
        } catch (PatternSyntaxException e) {
            throw new RuntimeException("Error parsing: " + queryTuple.getKey(), e);
        }

        return output;
    }
}

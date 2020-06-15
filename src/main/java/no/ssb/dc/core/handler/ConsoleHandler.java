package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Console;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

@Handler(forClass = Console.class)
public class ConsoleHandler extends AbstractNodeHandler<Console> {

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleHandler.class);

    public ConsoleHandler(Console node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        super.execute(context);
        Response response = context.state(Response.class);
        PageEntryState pageEntryState = context.state(PageEntryState.class);

        String variables = context.variables().entrySet().stream().map(entry -> String.format("%s=%s", entry.getKey(), entry.getValue())).collect(Collectors.joining(", "));

        String url = response == null ? "N/A" : response.url();
        int statusCode = response == null ? -1 : response.statusCode();
        byte[] responseBody = response == null ? new byte[0] : response.body();
        byte[] body = pageEntryState == null ? responseBody : pageEntryState.content;
        String bodyText = body == null ? "" : new String(body, StandardCharsets.UTF_8);

        LOG.debug("\nState:\n\tURL [{}]: {}\n\tVariables: {}\n\tBody: {}", statusCode, url, variables, bodyText);

        return ExecutionContext.empty();
    }

}

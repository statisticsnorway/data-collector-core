package no.ssb.dc.core.health;

import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.dc.api.health.HealthRenderPriority;
import no.ssb.dc.api.health.HealthResource;
import no.ssb.dc.api.util.JsonParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

@HealthRenderPriority(priority = 15)
public class HealthWorkerHistoryResource implements HealthResource {

    final CopyOnWriteArrayList<ObjectNode> historyList = new CopyOnWriteArrayList<>();

    @Override
    public Optional<Boolean> isUp() {
        return Optional.empty();
    }

    @Override
    public String name() {
        return "worker-history";
    }

    @Override
    public boolean isList() {
        return true;
    }

    @Override
    public boolean canRender(Map<String, Deque<String>> queryParams) {
        return true;
    }

    @Override
    public Object resource() {
        List<ObjectNode> reversedList = new ArrayList<>(historyList);
        Collections.reverse(reversedList);
        return reversedList;
    }

    public void add(HealthWorkerResource workerResource) {
        JsonParser jsonParser = JsonParser.createJsonParser();
        ObjectNode convertedNode = jsonParser.mapper().convertValue(workerResource.resource(), ObjectNode.class);
        historyList.add(convertedNode);
    }
}

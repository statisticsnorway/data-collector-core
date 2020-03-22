package no.ssb.dc.core.health;

import com.fasterxml.jackson.annotation.JsonProperty;
import no.ssb.dc.api.health.HealthRenderPriority;
import no.ssb.dc.api.health.HealthResource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

@HealthRenderPriority(priority = 50)
public class HealthThreadsResource implements HealthResource {

    @Override
    public Optional<Boolean> isUp() {
        return Optional.empty();
    }

    @Override
    public String name() {
        return "thread-info";
    }

    @Override
    public boolean isList() {
        return false;
    }

    @Override
    public boolean canRender(Map<String, Deque<String>> queryParams) {
        return Set.of("threads", "all").stream().anyMatch(queryParams::containsKey);
    }

    @Override
    public Object resource() {
        Set<Thread> threads = Thread.getAllStackTraces().keySet();

        SortedMap<String, List<ThreadInfo>> threadGroups = new TreeMap<>(Comparator.comparing(String::toString));
        for (Thread thread : threads) {
            String threadGroupName = Optional.ofNullable(thread.getThreadGroup()).map(ThreadGroup::getName).orElse("Not found");
            List<ThreadInfo> threadInfoList = threadGroups.computeIfAbsent(threadGroupName, list -> new ArrayList<>());

            String name = thread.getName();
            Thread.State state = thread.getState();
            int priority = thread.getPriority();
            String type = thread.isDaemon() ? "Daemon" : "Normal";
            List<String> stackTraceList = List.of(thread.getStackTrace()).stream().map(StackTraceElement::toString).collect(Collectors.toList());
            threadInfoList.add(new ThreadInfo(
                    thread.getId(),
                    name,
                    state,
                    priority,
                    thread.isAlive(),
                    thread.isInterrupted(),
                    type,
                    stackTraceList));

            threadGroups.put(threadGroupName, threadInfoList);
        }

        return new ThreadStatus(threadGroups.size(), threads.size(), threadGroups);
    }

    @SuppressWarnings("WeakerAccess")
    public static class ThreadStatus {
        @JsonProperty("thread-group-count") public final Integer threadGroupCount;
        @JsonProperty("thread-count") public final Integer threadCount;
        @JsonProperty("threads") public final Map<String, List<ThreadInfo>> threadInfoList;

        public ThreadStatus(Integer threadGroupCount, Integer threadCount, Map<String, List<ThreadInfo>> threadInfoMap) {
            this.threadGroupCount = threadGroupCount;
            this.threadCount = threadCount;
            this.threadInfoList = threadInfoMap;
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static class ThreadInfo {
        @JsonProperty public final Long id;
        @JsonProperty public final String name;
        @JsonProperty public final Thread.State state;
        @JsonProperty public final Integer priority;
        @JsonProperty public final Boolean alive;
        @JsonProperty public final Boolean interrupted;
        @JsonProperty public final String type;
        @JsonProperty("stackTrace") public final List<String> stackTraceElement;

        public ThreadInfo(Long id, String name, Thread.State state, Integer priority, Boolean alive, Boolean interrupted, String type, List<String> stackTraceElements) {
            this.id = id;
            this.name = name;
            this.state = state;
            this.priority = priority;
            this.alive = alive;
            this.interrupted = interrupted;
            this.type = type;
            this.stackTraceElement = stackTraceElements;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ThreadInfo that = (ThreadInfo) o;
            return Objects.equals(id, that.id) &&
                    state == that.state &&
                    Objects.equals(priority, that.priority) &&
                    Objects.equals(alive, that.alive) &&
                    Objects.equals(interrupted, that.interrupted) &&
                    Objects.equals(type, that.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, state, priority, alive, interrupted, type);
        }

        @Override
        public String toString() {
            return "ThreadInfo{" +
                    "id='" + id + '\'' +
                    ", state=" + state +
                    ", priority=" + priority +
                    ", alive=" + alive +
                    ", interrupted=" + interrupted +
                    ", type='" + type + '\'' +
                    '}';
        }
    }
}

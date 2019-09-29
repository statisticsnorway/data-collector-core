package no.ssb.dc.core.executor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class BufferedReordering<ELEMENT> {

    private final Object lock = new Object();
    private final List<ELEMENT> expected = new ArrayList<>();
    private final Set<ELEMENT> completed = new LinkedHashSet<>();

    public BufferedReordering() {
    }

    public List<ELEMENT> expected() {
        return expected;
    }

    public Set<ELEMENT> completed() {
        return completed;
    }

    public void addExpected(ELEMENT element) {
        synchronized (lock) {
            expected.add(element);
        }
    }

    public void addExpected(List<ELEMENT> elements) {
        synchronized (lock) {
            expected.addAll(elements);
        }
    }

    public void addCompleted(ELEMENT elements, Consumer<List<ELEMENT>> orderedElementsCallback) {
        List<ELEMENT> orderedElements = new ArrayList<>();
        synchronized (lock) {
            completed.add(elements);
            Iterator<ELEMENT> iterator = expected.iterator();
            while (iterator.hasNext()) {
                ELEMENT element = iterator.next();
                if (!completed.remove(element)) {
                    break;
                }
                iterator.remove();
                orderedElements.add(element);
            }
        }
        if (!orderedElements.isEmpty()) {
            orderedElementsCallback.accept(orderedElements);
        }
    }
}

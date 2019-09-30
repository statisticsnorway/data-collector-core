package no.ssb.dc.core.handler;

import java.util.Arrays;
import java.util.Objects;

public class PageEntryState {

    final Object nodeObject;
    final byte[] content;

    public PageEntryState(Object nodeObject, byte[] content) {
        this.nodeObject = nodeObject;
        this.content = content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageEntryState that = (PageEntryState) o;
        return nodeObject.equals(that.nodeObject) &&
                Arrays.equals(content, that.content);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(nodeObject);
        result = 31 * result + Arrays.hashCode(content);
        return result;
    }

}

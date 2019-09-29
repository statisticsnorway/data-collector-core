package no.ssb.dc.core.handler.state;

public enum QueryStateHolder {
    QUERY_DATA,            // input data to query
    QUERY_RESULT,          // output data from query
    EXPECTED_POSITIONS,    // from sequence
    ITEM_LIST_ITEM_DATA;   // from parallel (query variables)
}

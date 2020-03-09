package no.ssb.dc.core.metrics;

import no.ssb.dc.core.http.HttpClientAgent;

import java.lang.instrument.Instrumentation;

public class MetricsAgent {

    public static void premain(String agentArgs, Instrumentation inst) {
        HttpClientAgent.install(inst);
    }

}

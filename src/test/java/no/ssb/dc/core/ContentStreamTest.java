package no.ssb.dc.core;

import no.ssb.dc.api.content.ContentStream;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.dc.content.provider.rawdata.RawdataClientContentStream;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.Test;

import java.util.Map;

public class ContentStreamTest {

    @Test
    public void thatRawdataClient() {
        RawdataClient client = ProviderConfigurator.configure(Map.of(), "memory", RawdataClientInitializer.class);
        ContentStream contentStream = new RawdataClientContentStream(client);
        ContentStreamProducer producer = contentStream.producer("ns");

        ContentStreamBuffer.Builder builder = producer.builder();
        builder.position("1")
                .buffer("a", new byte[7], null)
                .buffer("b", new byte[5], null);

        producer.produce(builder);

        producer.publish("1");
    }
}

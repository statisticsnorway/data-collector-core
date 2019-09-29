package no.ssb.dc.core;

import no.ssb.dc.api.Builders;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.delegate.Tuple;
import no.ssb.dc.api.node.RegEx;
import no.ssb.dc.core.handler.Queries;
import org.testng.annotations.Test;

public class XPathTest {
    
    final String xml = "<?xml version=\"1.0\"?>" +
            "<feed>" +
            "    <id>" +
            "        http://folkeregisteret-api-konsument-playground.sits.no/folkeregisteret/offentlig-med-hjemmel/api/v1/hendelser/feed" +
            "    </id>" +
            "    <title>Offentlig hendelsesliste</title>" +
            "    <author>" +
            "        <name>Skatteetaten</name>" +
            "    </author>" +
            "    <link rel='self' type='application/atom+xml'" +
            "          href='http://folkeregisteret-api-konsument-playground.sits.no/folkeregisteret/offentlig-med-hjemmel/api/v1/hendelser/feed?seq=1'/>" +
            "    <link rel='first' type='application/atom+xml'" +
            "          href='http://folkeregisteret-api-konsument-playground.sits.no/folkeregisteret/offentlig-med-hjemmel/api/v1/hendelser/feed?seq=0'/>" +
            "    <link rel='next' type='application/atom+xml'" +
            "          href='http://folkeregisteret-api-konsument-playground.sits.no/folkeregisteret/offentlig-med-hjemmel/api/v1/hendelser/feed?seq=1001'/>" +
            "    <updated>2019-09-20T16:26:48.597Z</updated>" +
            "    <entry>" +
            "        <id>1</id>" +
            "    </entry>" +
            "    <entry>" +
            "        <id>2</id>" +
            "    </entry>" +
            "    <entry>" +
            "        <id>3</id>" +
            "    </entry>" +
            "</feed>";

    @Test
    public void testXpathHandler() {
        RegEx regex = Builders.regex(Builders.xpath("/feed/link[@rel=\"next\"]/@href"), "(?<=[?&]seq=)[^&]*").build();
        Tuple<Position<?>, String> nextPositionTuple = Queries.regex(regex, ExecutionContext.empty(), xml);
        System.out.printf("nextPosition: %s", nextPositionTuple);
    }

}

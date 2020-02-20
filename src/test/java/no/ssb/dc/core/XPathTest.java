package no.ssb.dc.core;

import no.ssb.dc.api.Builders;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.QueryFeature;
import no.ssb.dc.api.node.RegEx;
import no.ssb.dc.api.node.XPath;
import no.ssb.dc.core.handler.Queries;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

    String xml404withResponseError = "<?xml version='1.0' encoding='UTF-8'?>" +
            "<feil xmlns=\"urn:no:skatteetaten:datasamarbeid:feil:v1\">" +
            "    <kode>SM-002</kode>" +
            "    <melding>Fant ikke noen skattemelding for gitt år og personidentifikator</melding>" +
            "    <korrelasjonsid>b0e88d88ab83b3cd417d2ee88a696afb</korrelasjonsid>" +
            "</feil>";

    @Test
    public void testXpathHandler() {
        DocumentParserFeature parser = Queries.parserFor(XPath.class);
        assertNotNull(parser);
        Document document = (Document) parser.deserialize(xml.getBytes());
        assertNotNull(document);
        byte[] serialized = parser.serialize(document);
        assertArrayEquals(parser.serialize(parser.deserialize(serialized)), parser.serialize(document));

        RegEx regex = Builders.regex(Builders.xpath("/feed/link[@rel=\"next\"]/@href"), "(?<=[?&]seq=)[^&]*").build();
        String nextPosition = Queries.from(regex).evaluateStringLiteral(xml);
        System.out.printf("nextPosition: %s", nextPosition);
    }

    @Test
    void testXml404withResponseError() {
        XPath xPath = Builders.xpath("/feil/kode").build();
        QueryFeature qf = Queries.from(xPath);
        String result = qf.evaluateStringLiteral(xml404withResponseError);
        assertEquals(result, "SM-002");
    }
}

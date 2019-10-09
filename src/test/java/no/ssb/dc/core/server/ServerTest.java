package no.ssb.dc.core.server;

import io.undertow.Undertow;
import no.ssb.dc.test.client.TestClient;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerListener;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.List;

import static org.testng.Assert.assertEquals;

@Listeners(TestServerListener.class)
public class ServerTest {

    @Inject
    TestServer server;

    @Inject
    TestClient client;

    @Test
    public void thatTestServerStartsMainWithRunningUndertowListener() {
        List<Undertow.ListenerInfo> listenerInfo = server.getApplication().unwrap(Undertow.class).getListenerInfo();
        Undertow.ListenerInfo info = listenerInfo.iterator().next();
        assertEquals(info.getProtcol(), "http");
    }

    @Test
    public void testTestController() {
        client.get("/test").expect200Ok();
    }

    @Test
    public void testLoopbackController() {
        System.out.println(client.get("/echo").expect200Ok().body());
    }
}

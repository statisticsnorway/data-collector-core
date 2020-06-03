package no.ssb.dc.core.http;

import net.bytebuddy.agent.ByteBuddyAgent;
import no.ssb.dc.api.Builders;
import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Get;
import no.ssb.dc.api.services.Services;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.handler.GetHandler;
import no.ssb.dc.test.server.TestServerFactory;
import org.junit.jupiter.api.Test;

import javax.net.ServerSocketFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This test reproduces unknown-body-length that occurs in circumstances where the web server
 * responds with 200 and Content-length, but no content.
 */
public class UnknownBodyLengthTest {

    static SocketServer createServer(Phaser phaser) {
        SocketServer server = new SocketServer(phaser);
        Thread thread = new Thread(() -> server.serve(false));
        thread.start();
        return server;
    }

    @Test
    public void thatSocketServerFailsWithIOExceptionHttp1_0() throws TimeoutException, InterruptedException {
        Phaser phaser = new Phaser(1);
        SocketServer server = createServer(phaser);
        phaser.awaitAdvanceInterruptibly(0, 5, TimeUnit.SECONDS);
        Request request = Request.newRequestBuilder().url("http://localhost:" + server.port + "/test").GET().build();
        AtomicReference<Throwable> failureCause = new AtomicReference<>();
        CompletableFuture<Response> response = Client.newClient().sendAsync(request)
                .exceptionally(throwable -> {
                    failureCause.compareAndSet(null, throwable);
                    phaser.arrive();
                    return null;
                });
        phaser.awaitAdvanceInterruptibly(1, 5, TimeUnit.SECONDS);
        server.close();
        System.err.println("Expected error: " + CommonUtils.captureStackTrace(failureCause.get()));
    }

    @Test
    public void thatGetHandlerDealsWithIOExceptionHttp1_0() throws TimeoutException, InterruptedException {
        HttpClientAgent.install(ByteBuddyAgent.install());
        assertThrows(IllegalStateException.class, () -> {
            Phaser phaser = new Phaser(1);
            SocketServer server = createServer(phaser);
            phaser.awaitAdvanceInterruptibly(0, 30, TimeUnit.SECONDS);
            Get get = Builders.get("failTask").url("http://localhost:" + server.port + "/test").build();
            GetHandler handler = new GetHandler(get);

            ConfigurationMap configurationMap = new ConfigurationMap(new LinkedHashMap<>());
            configurationMap.put("data.collector.http.request.timeout.seconds", "15");
            Client client = Client.newClientBuilder().version(Client.Version.HTTP_2).build();
            Services services = Services.create()
                    .register(ConfigurationMap.class, configurationMap)
                    .register(Client.class, client);
            ExecutionContext input = new ExecutionContext.Builder()
                    .services(services)
                    .build();

            handler.execute(input);
            phaser.awaitAdvanceInterruptibly(1, 5, TimeUnit.SECONDS);
            server.close();
        });
    }

    // https://bugs.openjdk.java.net/browse/JDK-8210130
    // http://hg.openjdk.java.net/jdk/jdk/rev/40eb23e0a8c5
    static class SocketServer {
        static final byte[] BUF = new byte[32 * 10234 + 2];
        final ServerSocketFactory socketFactory = ServerSocketFactory.getDefault();
        final ServerSocket serverSocket;
        final int port;
        final Phaser phaser;
        final AtomicBoolean closed = new AtomicBoolean();

        SocketServer(Phaser phaser) {
            this.phaser = phaser;
            try {
                serverSocket = socketFactory.createServerSocket();
                serverSocket.setReuseAddress(true);
                port = TestServerFactory.findFreePort(new SecureRandom(), 9500, 9599);
                serverSocket.bind(new InetSocketAddress("127.0.0.1", port));
                System.out.println("ServerSocket = " + serverSocket.getClass() + " " + serverSocket);
                //port = serverSocket.getLocalPort();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void serve(final boolean withContentLength) {
            Arrays.fill(BUF, (byte) 0);
            try {
                phaser.arrive();
                while (!closed.get()) {
                    try (Socket socket = serverSocket.accept()) {
                        socket.setTcpNoDelay(true);
                        socket.setSoLinger(true, 1);
                        System.out.println("Accepted: " + socket.getRemoteSocketAddress());
                        System.out.println("Accepted: " + socket);
                        try (OutputStream out = socket.getOutputStream()) {
                            String response = "HTTP/1.0 200 OK\r\nConnection: close\r\nContent-Type: text/xml; charset=UTF-8\r\n";
                            out.write(response.getBytes());
                            String chdr = "Content-Length: " + BUF.length + "\r\n";
                            System.out.println(chdr);
                            if (withContentLength) {
                                out.write(chdr.getBytes());
                            }
                            out.write("\r\n".getBytes());
                            out.write(BUF);
                            out.flush();
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void close() {
            closed.set(true);
        }
    }
}

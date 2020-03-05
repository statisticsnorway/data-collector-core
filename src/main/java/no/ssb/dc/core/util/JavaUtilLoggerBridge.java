package no.ssb.dc.core.util;

import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.logging.LogManager;

public class JavaUtilLoggerBridge {

    private static class Initializer {
        static {
            LogManager.getLogManager().reset();
            SLF4JBridgeHandler.removeHandlersForRootLogger();
            SLF4JBridgeHandler.install();
        }

        private static void configure() {
            // method exists to allow access to static initializer
        }
    }

    public static final void installJavaUtilLoggerBridgeHandler() {
        Initializer.configure();
        //LogManager.getLogManager().getLogger("").setLevel(level);
    }
}

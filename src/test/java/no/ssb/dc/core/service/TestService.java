package no.ssb.dc.core.service;

import no.ssb.dc.application.spi.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(TestService.class);

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void start() {
        LOG.trace("Started..");
    }

    @Override
    public void stop() {
        LOG.trace("Stopped!");
    }
}

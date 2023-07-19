package com.datastax.astra.driver.examples;

import com.datastax.astra.driver.examples.common.ConnectionOptions;
import com.datastax.astra.driver.examples.common.Operations;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

/**
 * Sample app that connects to AstraDB using a Secure Connect Bundle, creates then populates a table.
 */
public class AstraSingleRegion {

    private static final Logger LOG = LoggerFactory.getLogger(AstraSingleRegion.class);

    // Entry point, parse args and call run
    public static void main(String[] args) {
        ConnectionOptions.fromArgs(AstraSingleRegion.class, args).ifPresent(AstraSingleRegion::run);
    }

    // Populate AstraDB using the provided connection options
    public static void run(ConnectionOptions options) {
        final String keyspace = options.getKeyspace();

        DriverConfigLoader config = DriverConfigLoader.fromClasspath("astra.conf");
        CqlSessionBuilder sessionBuilder = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(options.getAstraSecureConnectBundle()))
                .withAuthCredentials("token", options.getAstraToken())
                .withKeyspace(keyspace);

        LOG.debug("Creating connection using '{}'", options.getAstraSecureConnectBundle());
        LOG.debug("Using keyspace '{}'", keyspace);
        try (CqlSession cqlSession = Operations.connect(sessionBuilder, config)) {
            if (options.getIterations() == 0) {
                return;
            }
            Operations.runDemo(cqlSession, options.getIterations());
        }
    }
}

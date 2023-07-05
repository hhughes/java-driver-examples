package com.datastax.astra.driver.examples;

import com.datastax.astra.driver.examples.common.ConnectionOptions;
import com.datastax.astra.driver.examples.common.Operations;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

public class AstraSingleRegion {

    private static final Logger LOG = LoggerFactory.getLogger(AstraSingleRegion.class);

    /**
     * Sample app that connects to AstraDB using a Secure Connect Bundle, creates then populates a table.
     */
    public static void main(String[] args) {
        ConnectionOptions.fromArgs(AstraSingleRegion.class, args).ifPresent(options -> {
            final String keyspace = options.getKeyspace();

            DriverConfigLoader config = DriverConfigLoader.fromClasspath("astra.conf");
            CqlSessionBuilder sessionBuilder = CqlSession.builder()
                    .withCloudSecureConnectBundle(Paths.get(options.getAstraSecureConnectBundle()))
                    .withAuthCredentials("token", options.getAstraToken())
                    .withKeyspace(keyspace);

            LOG.debug("Creating connection using '{}'", options.getAstraSecureConnectBundle());
            LOG.debug("Using keyspace '{}'", keyspace);
            try (CqlSession cqlSession = connect(sessionBuilder, config)) {
                Operations.runDemo(cqlSession);
            }
        });
    }

    private static CqlSession connect(CqlSessionBuilder sessionBuilder, DriverConfigLoader config) {
        try {
            return sessionBuilder.withConfigLoader(config).build();
        } catch (AllNodesFailedException e) {
            LOG.warn("Failed to create session.", e);
            throw e;
        }
    }
}

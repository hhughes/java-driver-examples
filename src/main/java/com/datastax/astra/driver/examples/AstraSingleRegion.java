package com.datastax.astra.driver.examples;

import com.datastax.astra.driver.examples.common.ConnectionOptions;
import com.datastax.astra.driver.examples.common.Operations;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
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
        Thread.currentThread().setName("main()");
        ConnectionOptions.fromArgs(AstraSingleRegion.class, args).ifPresent(AstraSingleRegion::run);
        System.exit(0);
    }

    // Populate AstraDB using the provided connection options
    public static void run(ConnectionOptions options) {
        final String keyspace = options.getKeyspace();

        final String username = Strings.isNullOrEmpty(options.getAstraToken()) ? options.getClientId() : "token";
        final String password = Strings.isNullOrEmpty(options.getAstraToken()) ? options.getSecret() : options.getAstraToken();

        final Operations.OperationRequestTracker tracker = new Operations.OperationRequestTracker();

        DriverConfigLoader config = DriverConfigLoader.fromClasspath("astra.conf");
        CqlSessionBuilder sessionBuilder = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(options.getAstraSecureConnectBundle()))
                .withAuthCredentials(username, password)
                .withRequestTracker(tracker)
                .withKeyspace(keyspace);

        LOG.debug("Creating connection using '{}'", options.getAstraSecureConnectBundle());
        LOG.debug("Using keyspace '{}'", keyspace);
        try (CqlSession cqlSession = Operations.connect(sessionBuilder, config)) {
            if (options.getIterations() == 0) {
                return;
            }
            Operations.runDemo(cqlSession, options.getIterations(), tracker);
        }
    }
}

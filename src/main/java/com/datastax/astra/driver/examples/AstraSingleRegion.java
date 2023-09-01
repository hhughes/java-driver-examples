package com.datastax.astra.driver.examples;

import com.datastax.astra.driver.examples.common.ConnectionOptions;
import com.datastax.astra.driver.examples.common.Operations;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
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

        Cluster.Builder sessionBuilder = Cluster.builder()
                .withCloudSecureConnectBundle(Paths.get(options.getAstraSecureConnectBundle()).toFile())
                .withCredentials(options.getClientId(), options.getClientSecret())
                .withQueryOptions(new QueryOptions()
                        .setRefreshNodeIntervalMillis(100)
                        .setRefreshNodeListIntervalMillis(100));

        LOG.debug("Creating connection using '{}' [client-id: '{}' / client-secret: '{}']", options.getAstraSecureConnectBundle(), options.getClientId(), options.getClientSecret());
        LOG.debug("Using keyspace '{}'", keyspace);
        try (Session cqlSession = Operations.connect(sessionBuilder, keyspace)) {
            if (options.getIterations() == 0) {
                return;
            }
            Operations.runDemo(cqlSession, options.getIterations());
        }
    }
}

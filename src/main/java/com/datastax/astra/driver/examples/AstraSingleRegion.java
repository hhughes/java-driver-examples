package com.datastax.astra.driver.examples;

import com.datastax.astra.driver.examples.common.ConnectionOptions;
import com.datastax.astra.driver.examples.common.Operations;
import com.datastax.driver.core.Cluster;
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

        Cluster.Builder builder = Cluster.builder()
                .withCloudSecureConnectBundle(Paths.get(options.getAstraSecureConnectBundle()).toFile())
                .withCredentials("fkFXmtZFHeRrKNefFWBIDtNR", "hB7WvpemlOB89E4bM9Dh0NJTIjF69Ixi5jsIs1A2GM-xTZdlbweqcOBMc6LaQ9ZcS7HxBLi+db7ORpuTef-_c9PNkFaY,1v0YYZ-af1pkLLKCAuK1p6Az6eeg.+LZ2jc");

        LOG.debug("Creating connection using '{}'", options.getAstraSecureConnectBundle());
        LOG.debug("Using keyspace '{}'", keyspace);
        try (Session session = Operations.connect(builder, keyspace)) {
            Operations.runDemo(session, options.getIterations());
        }
    }
}

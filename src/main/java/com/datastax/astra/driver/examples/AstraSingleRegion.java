package com.datastax.astra.driver.examples;

import com.datastax.astra.driver.examples.common.ConnectionOptions;
import com.datastax.astra.driver.examples.beans.Connections;
import com.datastax.astra.driver.examples.common.Operations;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sample app that connects to AstraDB using a Secure Connect Bundle, creates then populates a table.
 */
public class AstraSingleRegion {

    private static final Logger LOG = LoggerFactory.getLogger(AstraSingleRegion.class);

    // Entry point, parse args and call run
    public static void main(String[] args) {
        Thread.currentThread().setName("demo-main");
        ConnectionOptions.fromArgs(AstraSingleRegion.class, args).ifPresent(AstraSingleRegion::run);
    }

    // Populate AstraDB using the provided connection options
    public static void run(ConnectionOptions options) {
        final String keyspace = options.getKeyspace();

        final String username = Strings.isNullOrEmpty(options.getAstraToken()) ? options.getClientId() : "token";
        final String password = Strings.isNullOrEmpty(options.getAstraToken()) ? options.getSecret() : options.getAstraToken();

        DriverConfigLoader config = DriverConfigLoader.fromClasspath("astra.conf");
        CqlSessionBuilder sessionBuilder = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(options.getAstraSecureConnectBundle()))
                .withAuthCredentials(username, password)
                .withKeyspace(keyspace);

        LOG.debug("Creating connection using '{}'", options.getAstraSecureConnectBundle());
        LOG.debug("Using keyspace '{}'", keyspace);
        try (CqlSession cqlSession = Operations.connect(sessionBuilder, config)) {
            if (options.getIterations() == 0) {
                return;
            }

            Map<Node, AtomicInteger> requestCounts = new ConcurrentHashMap<>();

            try {
                ObjectName objectName = new ObjectName("com.datastax.astra.driver.examples:type=basic,name=sessions");
                MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                server.registerMBean(new Connections(cqlSession, requestCounts, options.getAstraSecureConnectBundle()), objectName);
            } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException |
                     NotCompliantMBeanException e) {
                throw new RuntimeException(e);
            }

            Operations.runDemo(cqlSession, options.getIterations(), requestCounts);
        }
    }
}

package com.datastax.astra.driver.examples;

import com.datastax.astra.driver.examples.common.ConnectionOptions;
import com.datastax.astra.driver.examples.common.Operations;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import java.net.InetSocketAddress;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class LocalCluster {

    public static void main(String[] args) {
        ConnectionOptions.fromArgs(Operations.class, args).ifPresent(LocalCluster::run);
        System.exit(0);
    }

    public static void run(ConnectionOptions options) {
        DriverConfigLoader config = DriverConfigLoader.fromClasspath("astra.conf");
        CqlSessionBuilder sessionBuilder = CqlSession.builder()
                .withKeyspace(options.getKeyspace())
                .addContactPoint(InetSocketAddress.createUnresolved("127.0.0.1", 9042))
                .addContactPoint(InetSocketAddress.createUnresolved("127.0.0.2", 9042))
                .addContactPoint(InetSocketAddress.createUnresolved("127.0.0.3", 9042));
        try (CqlSession cqlSession = Operations.connect(sessionBuilder, config)) {
            Operations.runDemo(cqlSession, options.getIterations(), new Operations.OperationRequestTracker());
        }
    }

}

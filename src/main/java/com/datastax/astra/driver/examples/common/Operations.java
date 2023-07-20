package com.datastax.astra.driver.examples.common;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.Random;
import java.util.UUID;

public class Operations {
    private static final boolean USE_NEW_TABLE = false;
    private static final Logger LOG = LoggerFactory.getLogger(Operations.class);

    private static Statement buildCreateTableCql(String tableName) {
        // Idempotent create table
        return new SimpleStatement(String.format("CREATE TABLE IF NOT EXISTS %s (id uuid PRIMARY KEY, created_at timestamp, string text, number int)", tableName));
    }

    private static Statement buildDropTableCql(String tableName) {
        // Idempotent drop table
        return new SimpleStatement(String.format("DROP TABLE IF EXISTS %s", tableName));
    }

    private static class Entry {
        final UUID id;
        final String string;
        final int number;
        final float[] weights;

        public Entry(String string, int number, float[] weights) {
            this.id = UUID.randomUUID();
            this.string = string;
            this.number = number;
            this.weights = weights;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "id=" + id +
                    ", string='" + string + '\'' +
                    ", number=" + number +
                    ", weights=" + Arrays.toString(weights) +
                    '}';
        }
    }

    public static void runDemo(Session session, long iterations) {
        LOG.debug("Running demo with {} iterations", iterations);

        // Create new table to hold demo data (exit if it does)
        final String tableName = USE_NEW_TABLE ? String.format("demo_%s", UUID.randomUUID().toString().replaceAll("-", "_")) : "demo_singleton";

        Random r = new Random();

        try {
            if (USE_NEW_TABLE) {
                LOG.debug("Creating table '{}'", tableName);
                runWithRetries(session, buildCreateTableCql(tableName));
            }

            PreparedStatement preparedWrite = session.prepare(new SimpleStatement(String.format("INSERT INTO %s (id, created_at, string, number, weights) VALUES (?, ?, ?, ?, ?)", tableName)));
            PreparedStatement preparedReadEntry = session.prepare(new SimpleStatement(String.format("SELECT created_at, string, number, weights FROM %s WHERE id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", tableName)));

            LinkedList<UUID> ids = new LinkedList<>();

            int i=0;
            // intentional !=  check so that setting iterations < 0 will loop forever
            while (i != iterations) {
                // create new entry with random field values using prepared write statement
                Entry entry = new Entry(RandomStringUtils.randomAlphabetic(10), Math.abs(r.nextInt() % 9999), new float[] { r.nextFloat(), r.nextFloat(), r.nextFloat() });
                LOG.debug("Run {}: Inserting new entry {}", i++, entry);

                // bind variables from entry
                BoundStatement boundWrite = preparedWrite.bind(entry.id, Date.from(Instant.now()), entry.string, entry.number);

                runWithRetries(session, boundWrite);

                // accumulate new entry id and remove oldest if neccessary
                ids.add(entry.id);
                if (ids.size() > 10) {
                    ids.removeFirst();
                } else {
                    // cannot use prepare query until we have 10 items available
                    continue;
                }

                // read rows fetched using prepared read statement
                BoundStatement boundRead = preparedReadEntry.bind(ids.toArray());

                runWithRetries(session, boundRead)
                        .forEach(row -> LOG.debug("Received record ({}, {}, {})", row.getTimestamp("created_at"), row.getString("string"), row.getInt("number")));
            }
        } finally {
            if (USE_NEW_TABLE) {
                // if we are using a new table clean it up
                LOG.debug("Removing table '{}'", tableName);
                try {
                    runWithRetries(session, buildDropTableCql(tableName));
                } catch (Exception e) {
                    LOG.error("failed to clean up table", e);
                }
            }

            LOG.debug("Closing connection");
        }
    }

    public static ResultSet runWithRetries(Session session, Statement query) {
        // Queries will be retried indefinitely on timeout, they must be idempotent
        // In a real application there should be a limit to the number of retries
        while (true) {
            try {
                return session.execute(query);
            } catch (WriteTimeoutException | ReadTimeoutException | ConnectionException e) {
                // request timed-out, catch error and retry
                LOG.warn(String.format("Error '%s' executing query '%s', retrying", e.getMessage(), query), e);
            }
        }
    }

    public static Session connect(Cluster.Builder builder, String keyspace) {
        // Create the database connection session, retry connection failure an unlimited number of times
        // In a real application there should be a limit to the number of retries
        while (true) {
            try {
                return builder.build().connect(keyspace);
            } catch (IllegalStateException e) {
                // session creation failed, probably due to time-out, catch error and retry
                LOG.warn("Failed to create session.", e);
            }
        }
    }

    public static Session connect(Cluster.Builder builder, String primaryScb, String fallbackScb) {
        // Create the database connection session, retry connection failure an unlimited number of times
        // In a real application there should be a limit to the number of retries
        while (true) {
            try {
                return builder
                        .withCloudSecureConnectBundle(Paths.get(primaryScb).toFile())
                        .build()
                        .connect();
            } catch (IllegalStateException e) {
                // session creation failed, probably due to time-out, catch error and retry
                LOG.warn("Failed to create session.", e);
                return builder
                        .withCloudSecureConnectBundle(Paths.get(fallbackScb).toFile())
                        .build()
                        .connect();
            }
        }
    }
}

package com.datastax.astra.driver.examples.common;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;

public class Operations {
    private static final Logger LOG = LoggerFactory.getLogger(Operations.class);

    private static String buildCreateTableCql(String tableName) {
        // Idempotent create table
        return String.format("CREATE TABLE IF NOT EXISTS %s (id uuid PRIMARY KEY, title text, year int)", tableName);
    }

    private static String buildDropTableCql(String tableName) {
        // Idempotent drop table
        return String.format("DROP TABLE IF EXISTS %s", tableName);
    }

    private static String buildInsertMovieCql(String tableName, MovieEntry movie) {
        return String.format("INSERT INTO %s (id, title, year) VALUES (%s, '%s', %s)", tableName, movie.id, movie.title, movie.year);
    }

    private static String buildSelectAllCql(String tableName) {
        // Select specific columns, define limit for number of results
        return String.format("SELECT id, title, year FROM %s LIMIT 100", tableName);
    }

    private static class MovieEntry {
        final UUID id;
        final String title;
        final int year;

        public MovieEntry(String title, int year) {
            this.id = UUID.randomUUID();
            this.title = title;
            this.year = year;
        }
    }

    public static void runDemo(CqlSession session) {
        // Create new table to hold movie data (exit if it does)
        final String tableName = String.format("movies_%s", UUID.randomUUID().toString().replaceAll("-", "_"));

        try {
            LOG.debug("Creating table '{}'", tableName);
            runWithRetries(session, buildCreateTableCql(tableName));

            for (MovieEntry movie : Arrays.asList(
                    new MovieEntry("The Shawshank Redemption", 1994),
                    new MovieEntry("The Godfather", 1972),
                    new MovieEntry("The Dark Knight", 2008))) {
                LOG.debug("Adding record ({}, {}, {}) to '{}'", movie.id, movie.title, movie.year, tableName);
                runWithRetries(session, buildInsertMovieCql(tableName, movie));
            }

            LOG.debug("Listing all records in '{}'", tableName);
            for (Row row : runWithRetries(session, buildSelectAllCql(tableName))) {
                LOG.debug("Received record ({}, {}, {})", row.getUuid("id"), row.getString("title"), row.getInt("year"));
            }
        } finally {
            // Attempt to clean up the table
            LOG.debug("Removing table '{}'", tableName);
            runWithRetries(session, buildDropTableCql(tableName));
            LOG.debug("Closing connection");
        }
    }

    public static ResultSet runWithRetries(CqlSession session, String query) {
        // Queries will be retried indefinitely on timeout, they must be idempotent
        // In a real application there should be a limit to the number of retries
        while (true) {
            try {
                return session.execute(query);
            } catch (DriverTimeoutException e) {
                // request timed-out, catch error and retry
                LOG.warn(String.format("Timeout executing query '%s', retrying", query), e);
            }
        }
    }

    public static CqlSession connect(CqlSessionBuilder sessionBuilder, DriverConfigLoader primaryScbConfig) {
        // Create the database connection session, retry connection failure an unlimited number of times
        // In a real application there should be a limit to the number of retries
        while (true) {
            try {
                return sessionBuilder.withConfigLoader(primaryScbConfig).build();
            } catch (AllNodesFailedException e) {
                // session creation failed, probably due to time-out, catch error and retry
                LOG.warn("Failed to create session.", e);
            }
        }
    }

    public static CqlSession connect(CqlSessionBuilder sessionBuilder, String primaryScb, String fallbackScb, DriverConfigLoader staticConfig) {
        // Create the database connection session, retry connection failure an unlimited number of times
        // In a real application there should be a limit to the number of retries
        while (true) {
            try {
                return sessionBuilder
                        .withCloudSecureConnectBundle(Paths.get(primaryScb))
                        .withConfigLoader(staticConfig).build();
            } catch (AllNodesFailedException | IllegalStateException e) {
                // session creation failed, probably due to time-out, catch error and retry
                LOG.warn("Failed to create session.", e);
                return sessionBuilder
                        .withCloudSecureConnectBundle(Paths.get(fallbackScb))
                        .withConfigLoader(staticConfig).build();
            }
        }
    }
}

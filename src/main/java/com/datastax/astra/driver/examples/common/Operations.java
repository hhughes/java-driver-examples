package com.datastax.astra.driver.examples.common;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.UUID;

public class Operations {
    private static final Logger LOG = LoggerFactory.getLogger(Operations.class);

    private static String buildCreateTableCql(String tableName) {
        return String.format("CREATE TABLE %s (id uuid PRIMARY KEY, title text, year int)", tableName);
    }

    private static String buildDropTableCql(String tableName) {
        return String.format("DROP TABLE %s", tableName);
    }

    private static String buildInsertMovieCql(String tableName, MovieEntry movie) {
        return String.format("INSERT INTO %s (id, title, year) VALUES (%s, '%s', %s)", tableName, movie.id, movie.title, movie.year);
    }

    private static String buildSelectAllCql(String tableName) {
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

    public static void runDemo(CqlSession cqlSession) {
        // create new table to hold movie data (exit if it does)
        final String tableName = String.format("movies_%s", UUID.randomUUID().toString().replaceAll("-", "_"));

        try {
            LOG.debug("Creating table '{}'", tableName);
            cqlSession.execute(buildCreateTableCql(tableName));

            for (MovieEntry movie : Arrays.asList(
                    new MovieEntry("The Shawshank Redemption", 1994),
                    new MovieEntry("The Godfather", 1972),
                    new MovieEntry("The Dark Knight", 2008))) {
                LOG.debug("Adding record ({}, {}, {}) to '{}'", movie.id, movie.title, movie.year, tableName);
                cqlSession.execute(buildInsertMovieCql(tableName, movie));
            }

            LOG.debug("Listing all records in '{}'", tableName);
            for (Row row : cqlSession.execute(buildSelectAllCql(tableName))) {
                LOG.debug("Received record ({}, {}, {})", row.getUuid("id"), row.getString("title"), row.getInt("year"));
            }
        } finally {
            // attempt to clean up any created table
            LOG.debug("Removing table '{}'", tableName);
            cqlSession.execute(buildDropTableCql(tableName));
            LOG.debug("Closing connection");
        }
    }

}

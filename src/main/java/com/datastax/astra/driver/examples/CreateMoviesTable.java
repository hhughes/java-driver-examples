package com.datastax.astra.driver.examples;

import com.datastax.astra.driver.examples.common.ConnectionOptions;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.Row;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;

public class CreateMoviesTable
{
    /**
     * Sample app that connects to AstraDB using a Secure Connect Bundle, creates then populates a table.
     */
    public static void main(String[] args) {
        ConnectionOptions.fromArgs(CreateMoviesTable.class.getSimpleName(), args).ifPresent(options -> {
            final String keyspace = options.getKeyspace();

            CqlSessionBuilder sessionBuilder = CqlSession.builder()
                    .withCloudSecureConnectBundle(Paths.get(options.getAstraSecureConnectBundle()))
                    .withAuthCredentials("token", options.getAstraToken())
                    .withKeyspace(keyspace)
                    .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10L))
                            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM")
                            .withLong(DefaultDriverOption.REQUEST_PAGE_SIZE, 5000)
                            .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(10L))
                            .withDuration(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT, Duration.ofSeconds(10L))
                            .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(10L))
                            .build());

            System.out.printf("Creating connection using '%s'%n", options.getAstraSecureConnectBundle());
            System.out.printf("Using keyspace '%s'%n", keyspace);
            try (CqlSession cqlSession = sessionBuilder.build()) {
                // create new table to hold movie data (exit if it does)
                final String tableName = String.format("movies_%s", UUID.randomUUID().toString().replaceAll("-", "_"));
                if (cqlSession.getMetadata().getKeyspace(keyspace).get().getTable(tableName).isPresent()) {
                    System.out.printf("Error: Expected table '%s' to not exist, exiting", tableName);
                    System.exit(1);
                }

                try {
                    System.out.printf("Creating table '%s'%n", tableName);
                    cqlSession.execute(buildCreateTableCql(tableName));

                    for (MovieEntry movie : Arrays.asList(
                            new MovieEntry("The Shawshank Redemption", 1994),
                            new MovieEntry("The Godfather", 1972),
                            new MovieEntry("The Dark Knight", 2008))) {
                        System.out.printf("Adding record (%s, %s, %s) to '%s'%n", movie.id, movie.title, movie.year, tableName);
                        cqlSession.execute(buildInsertMovieCql(tableName, movie));
                    }

                    System.out.printf("Listing all records in '%s'%n", tableName);
                    for (Row row : cqlSession.execute(buildSelectAllCql(tableName))) {
                        System.out.printf("Received record (%s, %s, %s)%n", row.getUuid("id"), row.getString("title"), row.getInt("year"));
                    }
                } finally {
                    // attempt to clean up any created table
                    System.out.printf("Removing table '%s'%n", tableName);
                    cqlSession.execute(buildDropTableCql(tableName));
                    System.out.printf("Closing connection%n");
                }
            }
        });
    }

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
}

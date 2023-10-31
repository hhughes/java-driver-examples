package com.datastax.astra.driver.examples.common;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class Operations {
    private static final boolean USE_NEW_TABLE = false;
    private static final Logger LOG = LoggerFactory.getLogger(Operations.class);

    private static Statement buildCreateTableCql(String tableName) {
        // Idempotent create table
        return SimpleStatement.newInstance(String.format("CREATE TABLE IF NOT EXISTS %s (id uuid PRIMARY KEY, created_at timestamp, string text, number int)", tableName));
    }

    private static Statement buildDropTableCql(String tableName) {
        // Idempotent drop table
        return SimpleStatement.newInstance(String.format("DROP TABLE IF EXISTS %s", tableName));
    }

    private static class Entry {
        final UUID id;
        final String string;
        final int number;

        public Entry(String string, int number) {
            this.id = UUID.randomUUID();
            this.string = string;
            this.number = number;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "id=" + id +
                    ", string='" + trim(string) + '\'' +
                    ", number=" + number +
                    '}';
        }
    }

    public static void runDemo(CqlSession session, long iterations, OperationRequestTracker tracker) {
        LOG.debug("Running demo with {} iterations", iterations);

        // Create new table to hold demo data (exit if it does)
        final String tableName = USE_NEW_TABLE ? String.format("demo_%s", UUID.randomUUID().toString().replaceAll("-", "_")) : "demo_singleton";

        Random r = new Random();

        try {
            // attempt create whether we're using new table or not
            LOG.debug("Creating table '{}'", tableName);
            join(runWithRetries(session, buildCreateTableCql(tableName)));

            PreparedStatement preparedWrite = session.prepare(SimpleStatement.newInstance(String.format("INSERT INTO %s (id, created_at, string, number) VALUES (?, ?, ?, ?)", tableName)));
            PreparedStatement preparedRead = session.prepare(SimpleStatement.newInstance(String.format("SELECT created_at, string, number FROM %s WHERE id = ?", tableName)));

            for (int experiment=0; experiment<3; experiment++) {
                LOG.info("Preparing round {}", experiment);
                List<String> strings = IntStream.range(0, 200).mapToObj(i -> RandomStringUtils.randomAlphanumeric(10000)).collect(java.util.stream.Collectors.toList());
                LOG.info("Starting round {}", experiment);
                Queue<CompletionStage<Void>> operations = new ConcurrentLinkedQueue<>();
                long beginEnqueue = System.nanoTime();
                LongStream.range(0, iterations).forEach(i -> {
                    drainOperations(operations, 2000);

                    // create new entry with random field values using prepared write statement
                    Entry entry = new Entry(strings.get(r.nextInt(strings.size())), r.nextInt(10000));
                    LOG.debug("Run {}: Inserting new entry {}", i, entry);
                    CompletionStage<AsyncResultSet> writeResults = runWithRetries(session, preparedWrite.bind(entry.id, Instant.now(), entry.string, entry.number));

                    CompletionStage<AsyncResultSet> readResults = writeResults.thenComposeAsync(rs -> runWithRetries(session, preparedRead.bind(entry.id)));

                    CompletionStage<Void> result = readResults.thenAccept(rs -> rs.currentPage().forEach(row -> LOG.debug("Received record ({}, {}, {})", row.getInstant("created_at"), trim(row.getString("string")), row.getInt("number"))));
                    operations.add(result);
                });

                drainOperations(operations, 0);

                long end = System.nanoTime();
                long[] percentiles = tracker.percentiles(50, 90, 99);
                LOG.info("[{}] Processed {} queries in {} millis (p50/p90/p99: {}/{}/{} millis / {} failed)", experiment, iterations, TimeUnit.NANOSECONDS.toMillis(end - beginEnqueue), TimeUnit.NANOSECONDS.toMillis(percentiles[0]), TimeUnit.NANOSECONDS.toMillis(percentiles[1]), TimeUnit.NANOSECONDS.toMillis(percentiles[2]), tracker.errorCount.get());

                tracker.reset();
                Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
            }
        } finally {
            if (USE_NEW_TABLE) {
                // if we are using a new table clean it up
                LOG.debug("Removing table '{}'", tableName);
                try {
                    join(runWithRetries(session, buildDropTableCql(tableName)));
                } catch (Exception e) {
                    LOG.error("failed to clean up table", e);
                }
            }

            LOG.debug("Closing connection");
        }
    }

    private static String trim(String string) {
        return string.length() > 20 ? String.format("%s...", string.substring(0, 20)) : string;
    }

    private static void drainOperations(Queue<CompletionStage<Void>> operations, int threshold) {
        while (operations.size() > threshold) {
            operations.iterator().forEachRemaining(op -> {
                if (op.toCompletableFuture().isDone()) {
                    operations.remove(op);
                }
            });
            if (operations.size() > threshold) {
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MICROSECONDS);
            }
        }
    }

    private static void join(CompletionStage<AsyncResultSet> completionStage) {
        completionStage.toCompletableFuture().join();
    }

    public static CompletionStage<AsyncResultSet> runWithRetries(CqlSession session, Statement query) {
        // Queries will be retried indefinitely on timeout, they must be idempotent
        // In a real application there should be a limit to the number of retries
        while (true) {
            try {
                return session.executeAsync(query);
            } catch (DriverTimeoutException | WriteTimeoutException | ReadTimeoutException | ClosedConnectionException e) {
                // request timed-out, catch error and retry
                LOG.warn(String.format("Error '%s' executing query '%s', retrying", e.getMessage(), query), e);
            } catch (AllNodesFailedException e) {
                LOG.error(String.format("AllNodesFailedException error '%s' executing query '%s', retrying", e.getMessage(), query), e);
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

    public static class OperationRequestTracker implements RequestTracker {

        Queue<Long> timings = new ConcurrentLinkedQueue<>();
        AtomicLong errorCount = new AtomicLong(0);

        @Override
        public void onSuccess(@Nonnull Request request, long latencyNanos, @Nonnull DriverExecutionProfile executionProfile, @Nonnull Node node, @Nonnull String requestLogPrefix) {
            timings.add(latencyNanos);
            LOG.info("Operation success: {}", request);
        }

        @Override
        public void onError(@Nonnull Request request, @Nonnull Throwable error, long latencyNanos, @Nonnull DriverExecutionProfile executionProfile, @Nullable Node node, @Nonnull String requestLogPrefix) {
            errorCount.incrementAndGet();
            LOG.info("Operation failed: {}", error.getMessage());
        }

        @Override
        public void close() throws Exception { }

        private void reset() {
            timings.clear();
            errorCount.set(0);
        }

        private long[] percentiles(long ... p) {
            List<Long> snapshot = new ArrayList<>(timings);
            snapshot.sort(Long::compareTo);
            return LongStream.of(p).map(pct -> snapshot.get((int) Math.ceil((pct / 100.0) * snapshot.size()) - 1)).toArray();
        }
    }
}

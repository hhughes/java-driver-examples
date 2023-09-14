package com.datastax.astra.driver.examples;

import com.datastax.astra.driver.examples.common.ConnectionOptions;
import com.datastax.astra.driver.examples.common.Operations;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public class AstraMultiRegion {

    private static final Logger LOG = LoggerFactory.getLogger(AstraMultiRegion.class);

    // Entry point, parse args and call run
    public static void main(String[] args) {
        ConnectionOptions.fromArgs(AstraMultiRegion.class, args).ifPresent(AstraMultiRegion::run);
    }

    // Populate AstraDB using the provided connection options
    public static void run(ConnectionOptions options) {
        final String keyspace = options.getKeyspace();

        DriverConfigLoader staticConfig = DriverConfigLoader.fromClasspath("astra.conf");

        CqlSessionBuilder primaryBuilder = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(options.getAstraSecureConnectBundle()))
                .withAuthCredentials("token", options.getAstraToken())
                .withKeyspace(keyspace)
                .withConfigLoader(staticConfig);
        CqlSessionBuilder fallbackBuilder = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(options.getFallbackAstraSecureConnectBundle()))
                .withAuthCredentials("token", options.getAstraToken())
                .withKeyspace(keyspace)
                .withConfigLoader(staticConfig);

        LOG.info("Creating connection using '{}', fallback: '{}", options.getAstraSecureConnectBundle(), options.getFallbackAstraSecureConnectBundle());
        LOG.debug("Using keyspace '{}'", keyspace);
        try (PrimarySessionWithFallback cqlSession = new PrimarySessionWithFallback(primaryBuilder, fallbackBuilder)) {
            if (cqlSession.get() == null) {
                LOG.error("no session connected");
                throw new RuntimeException("no session connected");
            }
            // TODO: handle region-fallback after initial connection
            Operations.runDemo(cqlSession, options.getIterations(), new ConcurrentHashMap<>());
        }
    }

    /**
     * Class wrapping primary and fallback CqlSession definitions.
     * If connection cannot be established to primary, seamlessly try fallback.
     * Note: fallback only occurs at initialization, ideally this would also happen any time primary was closed.
     */
    public static class PrimarySessionWithFallback implements CqlSession {

        private CqlSession primary;
        private CqlSession fallback;

        public PrimarySessionWithFallback(CqlSessionBuilder primary, CqlSessionBuilder fallback) {
            CompletableFutures.getUninterruptibly(primary.buildAsync()
                    .handle((p, t) -> {
                        if (p != null) {
                            this.primary = p;
                            return CompletableFuture.completedFuture((CqlSession) null);
                        } else {
                            LOG.warn("failed to connect to primary, trying fallback", t);
                            return fallback.buildAsync();
                        }
                    })
                    .thenCompose(x -> x)
                    .handle((CqlSession f, Throwable t) -> {
                    if (f != null) {
                        this.fallback = f;
                    }
                    if (t != null) {
                        LOG.warn("failed to connect to fallback", t);
                    }
                    return CompletableFuture.completedFuture(null);
                }));
        }

        // use carefully - not thread-safe
        public CqlSession get() {
            return primary != null ? primary : fallback;
        }

        CompletionStage<Void> closeFuture;

        @NonNull
        @Override
        public CompletionStage<Void> closeFuture() {
            return closeFuture;
        }

        @NonNull
        @Override
        public CompletionStage<Void> closeAsync() {
            closeFuture = CompletableFuture.completedFuture(null);
            if (this.primary != null) {
                closeFuture = closeFuture.whenCompleteAsync((v, t) -> this.primary.closeAsync());
            }
            if (this.fallback != null) {
                closeFuture = closeFuture.whenCompleteAsync((v, t) -> this.fallback.closeAsync());
            }
            return closeFuture;
        }

        @NonNull
        @Override
        public CompletionStage<Void> forceCloseAsync() {
            return closeAsync();
        }

        @NonNull
        @Override
        public String getName() {
            return "PrimarySessionWithFallback";
        }

        @NonNull
        @Override
        public Metadata getMetadata() {
            return get().getMetadata();
        }

        @Override
        public boolean isSchemaMetadataEnabled() {
            return get().isSchemaMetadataEnabled();
        }

        @NonNull
        @Override
        public CompletionStage<Metadata> setSchemaMetadataEnabled(@Nullable Boolean newValue) {
            return get().setSchemaMetadataEnabled(newValue);
        }

        @NonNull
        @Override
        public CompletionStage<Metadata> refreshSchemaAsync() {
            return get().refreshSchemaAsync();
        }

        @NonNull
        @Override
        public CompletionStage<Boolean> checkSchemaAgreementAsync() {
            return get().checkSchemaAgreementAsync();
        }

        @NonNull
        @Override
        public DriverContext getContext() {
            return get().getContext();
        }

        @NonNull
        @Override
        public Optional<CqlIdentifier> getKeyspace() {
            return get().getKeyspace();
        }

        @NonNull
        @Override
        public Optional<Metrics> getMetrics() {
            return get().getMetrics();
        }

        @Nullable
        @Override
        public <RequestT extends Request, ResultT> ResultT execute(@NonNull RequestT request, @NonNull GenericType<ResultT> resultType) {
            return get().execute(request, resultType);
        }
    }
}

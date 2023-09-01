package com.datastax.astra.driver.examples.common;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Optional;

public class ConnectionOptions {

    private static Option ASTRA_SECURE_CONNECT_BUNDLE_OPTION = Option.builder()
            .longOpt("astraSecureConnectBundle")
            .argName("PATH")
            .desc("Path to Astra Secure Connect Bundle for the database you are connecting to. Downloaded from the Astra dashboard.")
            .hasArg().required().build();
    private static Option CLIENT_ID_OPTION = Option.builder()
            .longOpt("clientId")
            .argName("CLIENT_ID")
            .desc("Client ID for the specific User/Role who is connecting to the database.")
            .hasArg().build();
    private static Option CLIENT_SECRET_OPTION = Option.builder()
            .longOpt("clientSecret")
            .argName("CLIENT_SECRET")
            .desc("Client Secret for the specific User/Role who is connecting to the database.")
            .hasArg().build();
    private static Option KEYSPACE_OPTION = Option.builder()
            .longOpt("keyspace")
            .argName("KEYSPACE")
            .desc("Keyspace to use with this sample app. Note this may add or modify data already in this keyspace.")
            .hasArg().required().build();
    private static Option FALLBACK_ASTRA_SECURE_CONNECT_BUNDLE_OPTION = Option.builder()
            .longOpt("fallbackAstraSecureConnectBundle")
            .argName("PATH")
            .desc("Path to Astra Secure Connect Bundle for the fallback region you are connecting to. Downloaded from the Astra dashboard.")
            .hasArg().build();
    private static Option ITERATIONS_OPTION = Option.builder()
            .longOpt("iterations")
            .argName("N")
            .desc("Number of demo-loop iterations to perform (default=100)")
            .hasArg().build();

    private static Options OPTIONS = new Options()
            .addOption(ASTRA_SECURE_CONNECT_BUNDLE_OPTION)
            .addOption(CLIENT_ID_OPTION)
            .addOption(CLIENT_SECRET_OPTION)
            .addOption(KEYSPACE_OPTION)
            .addOption(FALLBACK_ASTRA_SECURE_CONNECT_BUNDLE_OPTION)
            .addOption(ITERATIONS_OPTION);

    public static Optional<ConnectionOptions> fromArgs(final Class mainClass, final String[] args) {
        final CommandLine commandLine;
        try {
            commandLine = new DefaultParser().parse(OPTIONS, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            new HelpFormatter().printHelp(mainClass.getSimpleName(), OPTIONS, true);
            return Optional.empty();
        }

        return Optional.of(new ConnectionOptions(
                commandLine.getOptionValue(ASTRA_SECURE_CONNECT_BUNDLE_OPTION),
                commandLine.getOptionValue(CLIENT_ID_OPTION),
                commandLine.getOptionValue(CLIENT_SECRET_OPTION),
                commandLine.getOptionValue(KEYSPACE_OPTION),
                commandLine.getOptionValue(FALLBACK_ASTRA_SECURE_CONNECT_BUNDLE_OPTION),
                commandLine.getOptionValue(ITERATIONS_OPTION)));
    }

    private final String astraSecureConnectBundle;
    private final String clientId;
    private final String clientSecret;
    private final String keyspace;
    private final String fallbackAstraSecureConnectBundle;
    private final long iterations;

    public ConnectionOptions(final String astraSecureConnectBundle,
                             final String clientId,
                             final String clientSecret,
                             final String keyspace,
                             String fallbackAstraSecureConnectBundle,
                             String iterations) {
        this.astraSecureConnectBundle = astraSecureConnectBundle;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.keyspace = keyspace;
        this.fallbackAstraSecureConnectBundle = fallbackAstraSecureConnectBundle;
        this.iterations = iterations != null && !iterations.isEmpty() ? Long.parseLong(iterations) : 100L;
    }

    public String getAstraSecureConnectBundle() {
        return this.astraSecureConnectBundle;
    }

    public String getClientId() {
        return this.clientId;
    }

    public String getClientSecret() {
        return this.clientSecret;
    }

    public boolean hasKeyspace() {
        return this.keyspace != null;
    }
    public String getKeyspace() {
        return this.keyspace;
    }

    public boolean hasFallbackAstraSecureConnectBundle() {
        return this.fallbackAstraSecureConnectBundle != null;
    }
    public String getFallbackAstraSecureConnectBundle() {
        return this.fallbackAstraSecureConnectBundle;
    }

    public long getIterations() {
        return this.iterations;
    }
}

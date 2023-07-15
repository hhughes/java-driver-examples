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
    private static Option ASTRA_TOKEN_OPTION = Option.builder()
            .longOpt("astraToken")
            .argName("TOKEN")
            .desc("Token for the specific User/Role who is connecting to the database. Begins with \"AstraCS:...\".")
            .hasArg().required().build();
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
            .addOption(ASTRA_TOKEN_OPTION)
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
                commandLine.getOptionValue(ASTRA_TOKEN_OPTION),
                commandLine.getOptionValue(KEYSPACE_OPTION),
                commandLine.getOptionValue(FALLBACK_ASTRA_SECURE_CONNECT_BUNDLE_OPTION),
                commandLine.getOptionValue(ITERATIONS_OPTION)));
    }

    private final String astraSecureConnectBundle;
    private final String astraToken;
    private final String keyspace;
    private final String fallbackAstraSecureConnectBundle;
    private final long iterations;

    public ConnectionOptions(final String astraSecureConnectBundle, final String astraToken, final String keyspace, String fallbackAstraSecureConnectBundle, String iterations) {
        this.astraSecureConnectBundle = astraSecureConnectBundle;
        this.astraToken = astraToken;
        this.keyspace = keyspace;
        this.fallbackAstraSecureConnectBundle = fallbackAstraSecureConnectBundle;
        this.iterations = iterations != null && !iterations.isEmpty() ? Long.parseLong(iterations) : 100L;
    }

    public String getAstraSecureConnectBundle() {
        return this.astraSecureConnectBundle;
    }
    public String getAstraToken() {
        return this.astraToken;
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

package com.datastax.astra.driver.examples;

import com.datastax.astra.driver.examples.common.ConnectionOptions;
import org.junit.Assume;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AstraDriverExamplesTest {

    private static String assumeProperty(String property) {
        String value = System.getProperty(property);
        Assume.assumeTrue(String.format("Test requires property %s to be set.", property), value != null && !value.isEmpty());
        return value;
    }

    @Test
    public void single_region_example() {
        String astraSecureConnectBundle = assumeProperty("args.astraSecureConnectBundle");
        String astraToken = assumeProperty("args.astraToken");
        String keyspace = assumeProperty("args.keyspace");

        AstraSingleRegion.run(new ConnectionOptions(astraSecureConnectBundle, astraToken, keyspace, null, "100"));
    }
}

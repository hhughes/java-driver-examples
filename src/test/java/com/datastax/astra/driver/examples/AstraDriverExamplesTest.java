package com.datastax.astra.driver.examples;

import static org.junit.Assert.assertTrue;

import com.datastax.astra.driver.examples.common.ConnectionOptions;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AstraDriverExamplesTest {
    @Test
    public void single_region_example() {
        AstraSingleRegion.run(new ConnectionOptions(System.getProperty("args.astraSecureConnectBundle"), System.getProperty("args.astraToken"), System.getProperty("args.keyspace"), null));
    }
}

/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KroxyConfigBuilderTest {

    @Test
    public void testBareConfig() {
        String config = new KroxyConfigBuilder("localhost:9192").build();
        assertEquals(config, """
                proxy:
                  address: "localhost:9192"
                clusters: {}
                filters: []
                """);
    }

    @Test
    public void testSslConfig() {
        String config = new KroxyConfigBuilder("localhost:9192")
                .withKeyStoreConfig("file", "pass").build();
        assertEquals(config, """
                proxy:
                  address: "localhost:9192"
                  keyStoreFile: "file"
                  keyPassword: "pass"
                clusters: {}
                filters: []
                """);
    }

    @Test
    public void testClusterConfig() {
        String config = new KroxyConfigBuilder("localhost:9192")
                .withDefaultCluster("localhost:9092").build();
        assertEquals(config, """
                proxy:
                  address: "localhost:9192"
                clusters:
                  demo:
                    bootstrap_servers: "localhost:9092"
                filters: []
                """);
    }

    @Test
    public void testTypeOnlyFilter() {
        String config = new KroxyConfigBuilder("localhost:9192")
                .addFilter("FilterType").build();
        assertEquals(config, """
                proxy:
                  address: "localhost:9192"
                clusters: {}
                filters:
                - type: "FilterType"
                """);
    }

    @Test
    public void testFilterWithSingleParam() {
        String config = new KroxyConfigBuilder("localhost:9192")
                .addFilter("FilterType", "a", "b").build();
        assertEquals(config, """
                proxy:
                  address: "localhost:9192"
                clusters: {}
                filters:
                - type: "FilterType"
                  config:
                    a: "b"
                """);
    }
}

/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.zjsonpatch.JsonDiff;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigurationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private ConfigParser configParser = new ConfigParser();

    public static Stream<Arguments> roundTrip() {
        return Stream.of(Arguments.of("Top level flags", """
                useIoUring: true
                """),
                Arguments.of("Virtual cluster (PortPerBroker)", """
                        virtualClusters:
                          demo1:
                            targetCluster:
                              bootstrap_servers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: PortPerBroker
                              config:
                                bootstrapAddress: cluster1:9192
                                numberOfBrokerPorts: 1
                                brokerAddressPattern: localhost:$(portNumber)
                                brokerStartPort: 9193
                        """),
                Arguments.of("Virtual cluster (SniRouting)", """
                        virtualClusters:
                          demo1:
                            targetCluster:
                              bootstrap_servers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: SniRouting
                              config:
                                bootstrapAddress: cluster1:9192
                                brokerAddressPattern: broker-$(nodeId):$(portNumber)
                        """),
                Arguments.of("Filters", """
                        filters:
                        - type: ProduceRequestTransformation
                          config:
                            transformation: io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter$UpperCasing
                        """),
                Arguments.of("Admin", """
                        adminHttp:
                          host: 0.0.0.0
                          port: 9193
                          endpoints: {}
                        """),
                Arguments.of("Micrometer", """
                        micrometer:
                        - type: CommonTags
                          config:
                            commonTags:
                              zone: "euc-1a"
                              owner: "becky"
                        """));

        /*
         *
         *
         * .withNewAdminHttp()
         * .withNewEndpoints()
         * .withPrometheusEndpointConfig(Map.of())
         * .endEndpoints()
         * .endAdminHttp();
         */
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    public void roundTrip(String name, String config) throws Exception {

        var configuration = configParser.parseConfiguration(config);
        var roundTripped = configParser.toYaml(configuration);

        var originalJsonNode = MAPPER.reader().readValue(config, JsonNode.class);
        var roundTrippedJsonNode = MAPPER.reader().readValue(roundTripped, JsonNode.class);
        var diff = JsonDiff.asJson(originalJsonNode, roundTrippedJsonNode);
        assertThat(diff).isEmpty();

    }

}
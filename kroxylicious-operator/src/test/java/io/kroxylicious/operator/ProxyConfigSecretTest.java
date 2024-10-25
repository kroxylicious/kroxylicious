/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.operator;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Secret;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyConfigSecretTest {

    @Test
    void test() throws IOException {
        // Given
        var kafkaProxy = Util.kafkaProxyFromResource("/KafkaProxy-example.yaml");

        // When
        Secret desired = new ProxyConfigSecret().desired(kafkaProxy, null);

        // Then
        assertThat(desired).isEqualTo(Util.YAML_MAPPER.readValue("""
                apiVersion: "v1"
                kind: "Secret"
                metadata:
                  name: "my-example-proxy"
                stringData:
                  config.yaml: |
                    adminHttp:
                      endpoints:
                        prometheus: {}
                    virtualClusters:
                      demo:
                        targetCluster:
                          bootstrap_servers: my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
                        clusterNetworkAddressConfigProvider:
                          type: PortPerBrokerClusterNetworkAddressConfigProvider
                          config:
                            bootstrapAddress: localhost:9292
                            brokerAddressPattern: my-example-proxy
                        logNetwork: false
                        logFrames: false
                """, Secret.class));
    }

}

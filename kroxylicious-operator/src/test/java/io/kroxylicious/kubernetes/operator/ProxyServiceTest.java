/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Service;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyServiceTest {

    @Test
    void test() throws IOException {
        // Given
        var kafkaProxy = Util.kafkaProxyFromResource("/KafkaProxy-example.yaml");

        // When
        Service desired = new ProxyService().desired(kafkaProxy, null);

        // Then
        assertThat(Util.YAML_MAPPER.writeValueAsString(desired)).isEqualTo("""
                apiVersion: "v1"
                kind: "Service"
                metadata:
                  name: "my-example-proxy"
                spec:
                  ports:
                  - name: "metrics"
                    port: 9190
                    protocol: "TCP"
                    targetPort: 9190
                  - name: "port-9292"
                    port: 9292
                    protocol: "TCP"
                    targetPort: 9292
                  - name: "port-9293"
                    port: 9293
                    protocol: "TCP"
                    targetPort: 9293
                  - name: "port-9294"
                    port: 9294
                    protocol: "TCP"
                    targetPort: 9294
                  - name: "port-9295"
                    port: 9295
                    protocol: "TCP"
                    targetPort: 9295
                  selector:
                    app: "kroxylicious"
                """);
    }

}

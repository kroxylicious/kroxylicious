/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.operator;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.apps.Deployment;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyDeploymentTest {

    @Test
    void test() throws IOException {
        // Given
        var kafkaProxy = Util.kafkaProxyFromResource("/KafkaProxy-example.yaml");

        // When
        Deployment desired = new ProxyDeployment().desired(kafkaProxy, null);

        // Then
        assertThat(Util.YAML_MAPPER.writeValueAsString(desired)).isEqualTo("""
                apiVersion: "apps/v1"
                kind: "Deployment"
                metadata:
                  labels:
                    app: "kroxylicious"
                  name: "my-example-proxy"
                spec:
                  replicas: 1
                  selector:
                    matchLabels:
                      app: "kroxylicious"
                  template:
                    metadata:
                      labels:
                        app: "kroxylicious"
                    spec:
                      containers:
                      - args:
                        - "--config"
                        - "/opt/kroxylicious/config/config.yaml"
                        image: "quay.io/kroxylicious/kroxylicious:0.9.0-SNAPSHOT"
                        name: "kroxylicious"
                        ports:
                        - containerPort: 9190
                          name: "metrics"
                        - containerPort: 9292
                        - containerPort: 9293
                        - containerPort: 9294
                        - containerPort: 9295
                        volumeMounts:
                        - mountPath: "/opt/kroxylicious/config/config.yaml"
                          name: "config-volume"
                          subPath: "config.yaml"
                      volumes:
                      - name: "config-volume"
                        secret:
                          secretName: "my-example-proxy"
                """);
    }

}

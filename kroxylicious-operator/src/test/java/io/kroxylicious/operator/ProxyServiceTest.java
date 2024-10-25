/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.operator;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.fabric8.kubernetes.api.model.Service;

import io.kroxylicious.crapi.v1alpha1.KafkaProxy;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyServiceTest {

    public static final YAMLMapper YAML_MAPPER = new YAMLMapper()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);

    private static KafkaProxy kafkaProxyFromString(String yaml) throws JsonProcessingException {
        // TODO should validate against the CRD schema, because the DependentResource
        // should never see an invalid resource in production
        return YAML_MAPPER.readValue(yaml, KafkaProxy.class);
    }

    @Test
    void test() throws JsonProcessingException {
        // Given
        var kafkaProxy = kafkaProxyFromString("""
                kind: KafkaProxy
                apiVersion: kroxylicious.io/v1alpha1
                metadata:
                  name: my-example-proxy
                spec:
                  bootstrapServers: my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
                """);

        // When
        Service desired = new ProxyService().desired(kafkaProxy, null);

        // Then
        assertThat(YAML_MAPPER.writeValueAsString(desired)).isEqualTo("""
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

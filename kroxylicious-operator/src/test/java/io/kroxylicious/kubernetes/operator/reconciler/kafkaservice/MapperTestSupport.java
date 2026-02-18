/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import java.util.List;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatusBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MapperTestSupport {

    public static final KafkaService SERVICE = new KafkaServiceBuilder()
            .withNewMetadata()
            .withName("test-service")
            .endMetadata()
            .withNewSpec()
            .withNewStrimziKafkaRef()
            .withNewRef()
            .withName("my-cluster")
            .withKind("Kafka")
            .withGroup("kafka.strimzi.io")
            .endRef()
            .endStrimziKafkaRef()
            .withNewTls()
            .withNewCertificateRef()
            .withName("my-secret")
            .endCertificateRef()
            .withNewTrustAnchorRef()
            .withNewRef()
            .withName("my-configmap")
            .endRef()
            .endTrustAnchorRef()
            .endTls()
            .endSpec()
            .build();

    public static final KafkaService SERVICE_SECRET_TRUST_ANCHOR = new KafkaServiceBuilder()
            .withNewMetadata()
            .withName("test-service")
            .endMetadata()
            .withNewSpec()
            .withNewStrimziKafkaRef()
            .withNewRef()
            .withName("my-cluster")
            .withKind("Kafka")
            .withGroup("kafka.strimzi.io")
            .endRef()
            .endStrimziKafkaRef()
            .withNewTls()
            .withNewCertificateRef()
            .withName("my-secret")
            .endCertificateRef()
            .withNewTrustAnchorRef()
            .withNewRef()
            .withKind("Secret")
            .withName("my-secret")
            .endRef()
            .endTrustAnchorRef()
            .endTls()
            .endSpec()
            .build();

    public static final ConfigMap TRUST_ANCHOR_PEM_CONFIG_MAP = new ConfigMapBuilder()
            .withNewMetadata()
            .withName("my-configmap")
            .withUid("uid")
            .withResourceVersion("7782")
            .endMetadata()
            .addToData("ca-bundle.pem", "value")
            .build();

    public static final Secret TRUST_ANCHOR_PEM_SECRET = new SecretBuilder()
            .withNewMetadata()
            .withName("my-secret")
            .withUid("uid")
            .withResourceVersion("7782")
            .endMetadata()
            .addToData("ca-bundle.pem", "value")
            .build();

    public static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
            .withName("my-cluster")
            .withUid("uid")
            .withResourceVersion("7782")
            .endMetadata()
            .withNewSpec()
            .withNewKafka()
            .withListeners(new GenericKafkaListenerBuilder()
                    .withName("plain")
                    .withTls(false)
                    .build())
            .endKafka()
            .endSpec()
            .withNewStatus()
            .withListeners(List.of(new ListenerStatusBuilder()
                    .withName("plain")
                    .withAddresses(new ListenerAddressBuilder()
                            .withPort(8080)
                            .withHost("localhost")
                            .build())
                    .build()))
            .endStatus()
            .build();

    public static final Secret TLS_SECRET = new SecretBuilder()
            .withNewMetadata()
            .withName("my-secret")
            .withUid("uid")
            .withResourceVersion("6744")
            .endMetadata()
            .withType("kubernetes.io/tls")
            .addToData("tls.crt", "value")
            .addToData("tls.key", "value")
            .build();

    public static KubernetesResourceList<KafkaService> mockKafkaServiceListOperation(KubernetesClient client) {
        MixedOperation<KafkaService, KubernetesResourceList<KafkaService>, Resource<KafkaService>> mockOperation = mock();
        when(client.resources(KafkaService.class)).thenReturn(mockOperation);
        KubernetesResourceList<KafkaService> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        return mockList;
    }
}

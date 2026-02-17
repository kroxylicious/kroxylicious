/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Arrays;
import java.util.UUID;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MapperTestSupport {

    public static final String NAMESPACE = "my-namespace";
    public static final String PROXY_NAME = "my-proxy";

    public static final String SERVER_CERT_SECRET_NAME = "server-cert";
    public static final String TRUST_ANCHOR_CERT_CONFIGMAP_NAME = "trust-anchor-cert";

    // @formatter:off
    public static final VirtualKafkaCluster CLUSTER_NO_FILTERS = new VirtualKafkaClusterBuilder()
            .withNewMetadata()
            .withName("foo")
            .withUid(UUID.randomUUID().toString())
            .withNamespace(NAMESPACE)
            .withGeneration(42L)
            .endMetadata()
            .withNewSpec()
            .withNewProxyRef()
            .withName(PROXY_NAME)
            .endProxyRef()
            .addNewIngress()
            .withNewIngressRef()
            .withName("my-ingress")
            .endIngressRef()
            .endIngress()
            .withNewTargetKafkaServiceRef()
            .withName("my-kafka")
            .endTargetKafkaServiceRef()
            .endSpec()
            .build();

    public static final VirtualKafkaCluster CLUSTER_TLS_NO_FILTERS = new VirtualKafkaClusterBuilder(CLUSTER_NO_FILTERS)
            .editOrNewSpec()
            .withIngresses(new IngressesBuilder(CLUSTER_NO_FILTERS.getSpec().getIngresses().get(0))
                    .withNewTls()
                    .withNewCertificateRef()
                    .withName(SERVER_CERT_SECRET_NAME)
                    .endCertificateRef()
                    .endTls()
                    .build())
            .endSpec()
            .build();

    public static final VirtualKafkaCluster CLUSTER_TLS_NO_FILTERS_WITH_TRUST_ANCHOR = new VirtualKafkaClusterBuilder(CLUSTER_NO_FILTERS)
            .editOrNewSpec()
            .withIngresses(new IngressesBuilder(CLUSTER_NO_FILTERS.getSpec().getIngresses().get(0))
                    .withNewTls()
                    .withNewCertificateRef()
                    .withName(SERVER_CERT_SECRET_NAME)
                    .endCertificateRef()
                    .withNewTrustAnchorRef()
                    .withNewRef()
                    .withName(TRUST_ANCHOR_CERT_CONFIGMAP_NAME)
                    .endRef()
                    .withKey("ca-bundle.pem")
                    .endTrustAnchorRef()
                    .endTls()
                    .build())
            .endSpec()
            .build();

    public static final Secret KUBE_TLS_CERT_SECRET = new SecretBuilder()
            .withNewMetadata()
            .withName(SERVER_CERT_SECRET_NAME)
            .withNamespace(NAMESPACE)
            .withUid(UUID.randomUUID().toString())
            .withGeneration(42L)
            .endMetadata()
            .withType("kubernetes.io/tls")
            .addToData("tls.crt", "value")
            .addToData("tls.key", "value")
            .build();

    public static final ConfigMap PEM_CONFIG_MAP = new ConfigMapBuilder()
            .withNewMetadata()
            .withName(TRUST_ANCHOR_CERT_CONFIGMAP_NAME)
            .withNamespace(NAMESPACE)
            .withGeneration(42L)
            .withUid(UUID.randomUUID().toString())
            .endMetadata()
            .addToData("ca-bundle.pem", "value")
            .build();

    public static EventSourceContext<VirtualKafkaCluster> mockContextContaining(VirtualKafkaCluster... clusters) {
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        KubernetesResourceList<VirtualKafkaCluster> mockList = mockListVirtualClustersOperation(client);
        when(mockList.getItems()).thenReturn(Arrays.asList(clusters));
        return eventSourceContext;
    }

    public static KubernetesResourceList<VirtualKafkaCluster> mockListVirtualClustersOperation(KubernetesClient client) {
        MixedOperation<VirtualKafkaCluster, KubernetesResourceList<VirtualKafkaCluster>, Resource<VirtualKafkaCluster>> mockOperation = mock();
        when(client.resources(VirtualKafkaCluster.class)).thenReturn(mockOperation);
        KubernetesResourceList<VirtualKafkaCluster> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        return mockList;
    }
}

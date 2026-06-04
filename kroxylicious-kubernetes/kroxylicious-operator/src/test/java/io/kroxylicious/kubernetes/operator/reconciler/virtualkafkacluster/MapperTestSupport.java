/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.source.IndexerResourceCache;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MapperTestSupport {

    public static final String NAMESPACE = "my-namespace";
    public static final String PROXY_NAME = "my-proxy";

    public static final String SERVER_CERT_SECRET_NAME = "server-cert";
    public static final String TRUST_ANCHOR_CERT_CONFIGMAP_NAME = "trust-anchor-cert";
    public static final String TRUST_ANCHOR_CERT_SECRET_NAME = "trust-anchor-secret";

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

    public static final VirtualKafkaCluster CLUSTER_TLS_NO_FILTERS_WITH_CONFIGMAP_TRUST_ANCHOR = new VirtualKafkaClusterBuilder(CLUSTER_TLS_NO_FILTERS)
            .editOrNewSpec()
                .editIngress(0)
                    .editOrNewTls()
                        .withNewTrustAnchorRef()
                            .withNewRef()
                                .withName(TRUST_ANCHOR_CERT_CONFIGMAP_NAME)
                            .endRef()
                            .withKey("ca-bundle.pem")
                        .endTrustAnchorRef()
                    .endTls()
                .endIngress()
            .endSpec()
            .build();

        public static final VirtualKafkaCluster CLUSTER_TLS_NO_FILTERS_WITH_SECRET_TRUST_ANCHOR = new VirtualKafkaClusterBuilder(CLUSTER_TLS_NO_FILTERS)
            .editOrNewSpec()
                .editIngress(0)
                    .editOrNewTls()
                        .withNewTrustAnchorRef()
                            .withNewRef()
                                .withKind("Secret")
                                .withName(TRUST_ANCHOR_CERT_SECRET_NAME)
                            .endRef()
                            .withKey("ca-bundle.pem")
                        .endTrustAnchorRef()
                    .endTls()
                .endIngress()
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

    public static final Secret TRUST_ANCHOR_PEM_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withName(TRUST_ANCHOR_CERT_SECRET_NAME)
                .withNamespace(NAMESPACE)
                .withGeneration(42L)
                .withUid(UUID.randomUUID().toString())
            .endMetadata()
            .addToData("ca-bundle.pem", "value")
            .build();
    // @formatter:on

    /**
     * Mocks an {@link EventSourceContext} whose primary cache contains the given clusters. Secondary&rarr;primary
     * mappers resolve referrers from this cache (via {@code findReferringPrimaries}) rather than issuing a live
     * {@code list} against the API server, so the mock stubs {@link EventSourceContext#getPrimaryCache()} and applies
     * the caller's predicate to the supplied clusters.
     */
    public static EventSourceContext<VirtualKafkaCluster> mockContextContaining(VirtualKafkaCluster... clusters) {
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mock();
        IndexerResourceCache<VirtualKafkaCluster> primaryCache = mock();
        when(eventSourceContext.getPrimaryCache()).thenReturn(primaryCache);
        when(primaryCache.list(any(), any())).thenAnswer(invocation -> {
            Predicate<VirtualKafkaCluster> predicate = invocation.getArgument(1);
            return Stream.of(clusters).filter(predicate);
        });
        return eventSourceContext;
    }
}

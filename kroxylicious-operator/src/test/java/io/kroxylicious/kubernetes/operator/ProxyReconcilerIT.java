/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assumptions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.operator.config.RuntimeDecl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ProxyReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyReconcilerIT.class);

    public static final String RESOURCE_NAME = "test-proxy";
    public static final String CLUSTER_FOO = "foo";
    public static final String CLUSTER_FOO_BOOTSTRAP = "my-cluster-kafka-bootstrap.foo.svc.cluster.local:9092";
    public static final String CLUSTER_BAR = "bar";
    public static final String CLUSTER_BAR_BOOTSTRAP = "my-cluster-kafka-bootstrap.bar.svc.cluster.local:9092";
    public static final String NEW_BOOTSTRAP = "new-bootstrap:9092";
    public static final String CLUSTER_BAZ = "baz";
    public static final String CLUSTER_BAZ_BOOTSTRAP = "my-cluster-kafka-bootstrap.baz.svc.cluster.local:9092";

    static KubernetesClient client;

    @BeforeAll
    static void checkKubeAvailable() {
        client = OperatorTestUtils.kubeClientIfAvailable();
        Assumptions.assumeThat(client).describedAs("Test requires a viable kube client").isNotNull();
    }

    @RegisterExtension
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new ProxyReconciler(new RuntimeDecl(List.of(
            // new FilterKindDecl("filter.kroxylicious.io", "v1alpha1", "RecordEncryption", "io.kroxylicious.filter.encryption.RecordEncryption")
            ))))
            .withKubernetesClient(client)
            .withAdditionalCustomResourceDefinition(VirtualKafkaCluster.class)
            .waitForNamespaceDeletion(true)
            .withConfigurationService(x -> x.withCloseClientOnStop(false))
            .build();

    @AfterEach
    void stopOperator() {
        extension.getOperator().stop();
    }

    @Test
    void testCreate() {
        doCreate();
    }

    private record CreatedResources(KafkaProxy proxy, Set<VirtualKafkaCluster> clusters) {
        public VirtualKafkaCluster cluster(String name) {
            return clusters.stream().filter(c -> c.getMetadata().getName().equals(name)).findFirst().orElseThrow();
        }
    }

    CreatedResources doCreate() {
        KafkaProxy proxy = testResource();
        final var cr = extension.create(proxy);
        VirtualKafkaCluster clusterFoo = extension.create(new VirtualKafkaClusterBuilder().withNewMetadata().withName(CLUSTER_FOO).endMetadata()
                .withNewSpec()
                .withNewTargetCluster()
                .withNewBootstrapping().withBootstrap(CLUSTER_FOO_BOOTSTRAP).endBootstrapping()
                .endTargetCluster()
                .withNewProxyRef().withName(proxy.getMetadata().getName()).endProxyRef()
                .withFilters()
                .endSpec().build());
        VirtualKafkaCluster clusterBar = extension.create(new VirtualKafkaClusterBuilder().withNewMetadata().withName(CLUSTER_BAR).endMetadata()
                .withNewSpec()
                .withNewTargetCluster()
                .withNewBootstrapping().withBootstrap(CLUSTER_BAR_BOOTSTRAP).endBootstrapping()
                .endTargetCluster()
                .withNewProxyRef().withName(proxy.getMetadata().getName()).endProxyRef()
                .withFilters()
                .endSpec().build());
        Set<VirtualKafkaCluster> clusters = Set.of(clusterFoo, clusterBar);

        await().alias("Secret as expected").untilAsserted(() -> {
            var secret = extension.get(Secret.class, ProxyConfigSecret.secretName(cr));
            assertThat(secret)
                    .isNotNull()
                    .extracting(ProxyReconcilerIT::decodeSecretData, InstanceOfAssertFactories.map(String.class, String.class))
                    .containsKey(ProxyConfigSecret.CONFIG_YAML_KEY)
                    .extracting(map -> map.get(ProxyConfigSecret.CONFIG_YAML_KEY), InstanceOfAssertFactories.STRING)
                    .contains(CLUSTER_FOO_BOOTSTRAP)
                    .contains(CLUSTER_BAR_BOOTSTRAP);
        });
        await().alias("Deployment as expected").untilAsserted(() -> {
            var deployment = extension.get(Deployment.class, ProxyDeployment.deploymentName(cr));
            assertThat(deployment).isNotNull()
                    .extracting(dep -> dep.getSpec().getTemplate().getSpec().getVolumes(), InstanceOfAssertFactories.list(Volume.class))
                    .describedAs("Deployment template should mount the proxy config secret")
                    .anyMatch(vol -> vol.getSecret() != null
                            && vol.getSecret().getSecretName().equals(ProxyConfigSecret.secretName(cr)));
        });
        await().alias("cluster Services as expected").untilAsserted(() -> {
            for (var cluster : clusters) {
                var service = extension.get(Service.class, ClusterService.serviceName(cluster));
                assertThat(service).isNotNull()
                        .extracting(svc -> svc.getSpec().getSelector())
                        .describedAs("Service's selector should select proxy pods")
                        .isEqualTo(ProxyDeployment.podLabels());
            }
        });

        return new CreatedResources(proxy, clusters);
    }

    @Test
    void testDelete() {
        var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;
        extension.delete(proxy);

        await().alias("Secret was deleted").untilAsserted(() -> {
            var secret = extension.get(Secret.class, ProxyConfigSecret.secretName(proxy));
            assertThat(secret).isNull();
        });
        await().alias("Deployment was deleted").untilAsserted(() -> {
            var deployment = extension.get(Deployment.class, ProxyDeployment.deploymentName(proxy));
            assertThat(deployment).isNull();
        });
        await().alias("Services were deleted").untilAsserted(() -> {
            for (var cluster : createdResources.clusters) {
                var service = extension.get(Service.class, ClusterService.serviceName(cluster));
                assertThat(service).isNull();
            }
        });
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void testUpdateVirtualCluster() {
        final var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;
        VirtualKafkaCluster cluster = createdResources.cluster(CLUSTER_FOO).edit().editSpec().editTargetCluster().editBootstrapping().withBootstrap(NEW_BOOTSTRAP)
                .endBootstrapping().endTargetCluster().endSpec().build();
        extension.replace(cluster);
        await().untilAsserted(() -> {
            var secret = extension.get(Secret.class, ProxyConfigSecret.secretName(proxy));
            assertThat(secret)
                    .isNotNull()
                    .extracting(ProxyReconcilerIT::decodeSecretData, InstanceOfAssertFactories.map(String.class, String.class))
                    .containsKey(ProxyConfigSecret.CONFIG_YAML_KEY)
                    .extracting(map -> map.get(ProxyConfigSecret.CONFIG_YAML_KEY), InstanceOfAssertFactories.STRING)
                    .doesNotContain(CLUSTER_FOO_BOOTSTRAP)
                    .contains(NEW_BOOTSTRAP);
        });

        await().untilAsserted(() -> {
            assertClusterServiceExists(CLUSTER_FOO);
            assertClusterServiceExists(CLUSTER_BAR);
        });

        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void testAddVirtualCluster() {
        final var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;
        extension.create(new VirtualKafkaClusterBuilder().withNewMetadata().withName(CLUSTER_BAZ).endMetadata()
                .withNewSpec()
                .withNewTargetCluster()
                .withNewBootstrapping().withBootstrap(CLUSTER_BAZ_BOOTSTRAP).endBootstrapping()
                .endTargetCluster()
                .withNewProxyRef().withName(proxy.getMetadata().getName()).endProxyRef()
                .withFilters()
                .endSpec().build());
        await().untilAsserted(() -> {
            var secret = extension.get(Secret.class, ProxyConfigSecret.secretName(proxy));
            assertThat(secret)
                    .isNotNull()
                    .extracting(ProxyReconcilerIT::decodeSecretData, InstanceOfAssertFactories.map(String.class, String.class))
                    .containsKey(ProxyConfigSecret.CONFIG_YAML_KEY)
                    .extracting(map -> map.get(ProxyConfigSecret.CONFIG_YAML_KEY), InstanceOfAssertFactories.STRING)
                    .contains(CLUSTER_BAZ_BOOTSTRAP);
        });

        await().untilAsserted(() -> {
            assertClusterServiceExists(CLUSTER_FOO);
            assertClusterServiceExists(CLUSTER_BAR);
            assertClusterServiceExists(CLUSTER_BAZ);
        });

        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void testDeleteVirtualCluster() {
        final var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;
        extension.delete(createdResources.cluster(CLUSTER_FOO));
        await().untilAsserted(() -> {
            var secret = extension.get(Secret.class, ProxyConfigSecret.secretName(proxy));
            assertThat(secret)
                    .isNotNull()
                    .extracting(ProxyReconcilerIT::decodeSecretData, InstanceOfAssertFactories.map(String.class, String.class))
                    .containsKey(ProxyConfigSecret.CONFIG_YAML_KEY)
                    .extracting(map -> map.get(ProxyConfigSecret.CONFIG_YAML_KEY), InstanceOfAssertFactories.STRING)
                    .doesNotContain(CLUSTER_FOO_BOOTSTRAP)
                    .contains(CLUSTER_BAR_BOOTSTRAP);
        });

        await().untilAsserted(() -> {
            var service = extension.get(Service.class, CLUSTER_FOO);
            assertThat(service)
                    .describedAs("Expect Service for cluster 'foo' to have been deleted")
                    .isNull();
        });

        await().untilAsserted(() -> {
            assertClusterServiceExists(CLUSTER_BAR);
        });
        LOGGER.atInfo().log("Test finished");
    }

    private static Map<String, String> decodeSecretData(Secret cm) {
        return cm.getData().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new String(Base64.getDecoder().decode(entry.getValue()))));
    }

    KafkaProxy testResource() {
        // @formatter:off
        return new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                .endMetadata()
                .build();
        // @formatter:on
    }

    private void assertClusterServiceExists(String clusterName) {
        var service = extension.get(Service.class, clusterName);
        assertThat(service)
                .describedAs("Expect Service for cluster '" + clusterName + "' to still exist")
                .isNotNull()
                .extracting(svc -> svc.getSpec().getSelector())
                .describedAs("Service's selector should select proxy pods")
                .isEqualTo(ProxyDeployment.podLabels());
    }

}

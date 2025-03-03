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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assumptions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.targetcluster.ClusterRefBuilder;
import io.kroxylicious.kubernetes.operator.config.RuntimeDecl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ProxyReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyReconcilerIT.class);

    public static final String PROXY_A = "proxy-a";
    public static final String PROXY_B = "proxy-b";
    public static final String CLUSTER_FOO_REF = "fooref";
    public static final String CLUSTER_FOO = "foo";
    public static final String CLUSTER_FOO_BOOTSTRAP = "my-cluster-kafka-bootstrap.foo.svc.cluster.local:9092";
    public static final String CLUSTER_BAR_REF = "barref";
    public static final String CLUSTER_BAR = "bar";
    public static final String CLUSTER_BAR_BOOTSTRAP = "my-cluster-kafka-bootstrap.bar.svc.cluster.local:9092";
    public static final String NEW_BOOTSTRAP = "new-bootstrap:9092";
    public static final String CLUSTER_BAZ = "baz";
    public static final String CLUSTER_BAZ_REF = "bazref";
    public static final String CLUSTER_BAZ_BOOTSTRAP = "my-cluster-kafka-bootstrap.baz.svc.cluster.local:9092";

    static KubernetesClient client;

    @BeforeAll
    static void checkKubeAvailable() {
        client = OperatorTestUtils.kubeClientIfAvailable();
        Assumptions.assumeThat(client).describedAs("Test requires a viable kube client").isNotNull();
        preloadOperatorImage();
    }

    // the initial operator image pull can take a long time and interfere with the tests
    private static void preloadOperatorImage() {
        String operandImage = ProxyDeployment.getOperandImage();
        Pod pod = client.run().withName("preload-operator-image")
                .withNewRunConfig()
                .withImage(operandImage)
                .withRestartPolicy("Never")
                .withCommand("ls").done();
        client.resource(pod).waitUntilCondition(it -> it.getStatus().getPhase().equals("Succeeded"), 2, TimeUnit.MINUTES);
        client.resource(pod).delete();
    }

    @RegisterExtension
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new ProxyReconciler(new RuntimeDecl(List.of(
            // new FilterKindDecl("filter.kroxylicious.io", "v1alpha1", "RecordEncryption", "io.kroxylicious.filter.encryption.RecordEncryption")
            ))))
            .withKubernetesClient(client)
            .withAdditionalCustomResourceDefinition(VirtualKafkaCluster.class)
            .withAdditionalCustomResourceDefinition(KafkaClusterRef.class)
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

    private record CreatedResources(KafkaProxy proxy, Set<VirtualKafkaCluster> clusters, Set<KafkaClusterRef> clusterRefs) {
        public VirtualKafkaCluster cluster(String name) {
            return clusters.stream().filter(c -> c.getMetadata().getName().equals(name)).findFirst().orElseThrow();
        }

        public KafkaClusterRef clusterRef(String name) {
            return clusterRefs.stream().filter(c -> c.getMetadata().getName().equals(name)).findFirst().orElseThrow();
        }
    }

    CreatedResources doCreate() {
        KafkaProxy proxy = extension.create(kafkaProxy(PROXY_A));
        KafkaClusterRef fooClusterRef = extension.create(clusterRef(CLUSTER_FOO_REF, CLUSTER_FOO_BOOTSTRAP));
        KafkaClusterRef barClusterRef = extension.create(clusterRef(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP));
        Set<KafkaClusterRef> clusterRefs = Set.of(fooClusterRef, barClusterRef);

        VirtualKafkaCluster clusterFoo = extension.create(virtualKafkaCluster(CLUSTER_FOO, proxy, fooClusterRef));
        VirtualKafkaCluster clusterBar = extension.create(virtualKafkaCluster(CLUSTER_BAR, proxy, barClusterRef));
        Set<VirtualKafkaCluster> clusters = Set.of(clusterFoo, clusterBar);

        assertProxyConfigContents(proxy, Set.of(CLUSTER_FOO_BOOTSTRAP, CLUSTER_BAR_BOOTSTRAP), Set.of());
        assertDeploymentMountsConfigSecret(proxy);
        assertDeploymentBecomesReady(proxy);
        assertServiceTargetsProxyInstances(proxy, clusters);
        return new CreatedResources(proxy, clusters, clusterRefs);
    }

    private void assertDeploymentBecomesReady(KafkaProxy proxy) {
        // wait longer for initial operator image download
        await().alias("Deployment as expected").untilAsserted(() -> {
            var deployment = extension.get(Deployment.class, ProxyDeployment.deploymentName(proxy));
            assertThat(deployment).isNotNull()
                    .extracting(Deployment::getStatus)
                    .describedAs("All deployment replicas should become ready")
                    .satisfies(status -> {
                        assertThat(status.getReplicas()).isEqualTo(status.getReadyReplicas());
                    });
        });
    }

    private void assertServiceTargetsProxyInstances(KafkaProxy proxy, Set<VirtualKafkaCluster> clusters) {
        await().alias("cluster Services as expected").untilAsserted(() -> {
            for (var cluster : clusters) {
                var service = extension.get(Service.class, ClusterService.serviceName(cluster));
                assertThat(service).isNotNull()
                        .extracting(svc -> svc.getSpec().getSelector())
                        .describedAs("Service's selector should select proxy pods")
                        .isEqualTo(ProxyDeployment.podLabels(proxy));
                // TODO shouldn't there be some identifier for the proxy here? The service will target all proxies.
            }
        });
    }

    private void assertDeploymentMountsConfigSecret(KafkaProxy proxy) {
        await().alias("Deployment as expected").untilAsserted(() -> {
            var deployment = extension.get(Deployment.class, ProxyDeployment.deploymentName(proxy));
            assertThat(deployment).isNotNull()
                    .extracting(dep -> dep.getSpec().getTemplate().getSpec().getVolumes(), InstanceOfAssertFactories.list(Volume.class))
                    .describedAs("Deployment template should mount the proxy config secret")
                    .anyMatch(vol -> vol.getSecret() != null
                            && vol.getSecret().getSecretName().equals(ProxyConfigSecret.secretName(proxy)));
        });
    }

    private void assertProxyConfigContents(KafkaProxy cr, Set<String> contains, Set<String> notContains) {
        await().alias("Secret as expected").untilAsserted(() -> {
            var secret = extension.get(Secret.class, ProxyConfigSecret.secretName(cr));
            AbstractStringAssert<?> extracting = assertThat(secret)
                    .isNotNull()
                    .extracting(ProxyReconcilerIT::decodeSecretData, InstanceOfAssertFactories.map(String.class, String.class))
                    .containsKey(ProxyConfigSecret.CONFIG_YAML_KEY)
                    .extracting(map -> map.get(ProxyConfigSecret.CONFIG_YAML_KEY), InstanceOfAssertFactories.STRING);
            if (!contains.isEmpty()) {
                extracting.contains(contains);
            }
            if (!notContains.isEmpty()) {
                extracting.doesNotContain(notContains);
            }
        });
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
    void testUpdateVirtualClusterTargetBootstrap() {
        final var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;
        var clusterRef = createdResources.clusterRef(CLUSTER_FOO_REF).edit().editSpec().withBootstrapServers(NEW_BOOTSTRAP).endSpec().build();
        extension.replace(clusterRef);

        assertDeploymentBecomesReady(proxy);
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
            assertClusterServiceExists(proxy, CLUSTER_FOO);
            assertClusterServiceExists(proxy, CLUSTER_BAR);
        });

        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void testAddVirtualCluster() {
        final var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;
        KafkaClusterRef bazClusterRef = extension.create(clusterRef(CLUSTER_BAZ_REF, CLUSTER_BAZ_BOOTSTRAP));
        extension.create(virtualKafkaCluster(CLUSTER_BAZ, proxy, bazClusterRef));
        await().untilAsserted(() -> {
            var secret = extension.get(Secret.class, ProxyConfigSecret.secretName(proxy));
            assertThat(secret)
                    .isNotNull()
                    .extracting(ProxyReconcilerIT::decodeSecretData, InstanceOfAssertFactories.map(String.class, String.class))
                    .containsKey(ProxyConfigSecret.CONFIG_YAML_KEY)
                    .extracting(map -> map.get(ProxyConfigSecret.CONFIG_YAML_KEY), InstanceOfAssertFactories.STRING)
                    .contains(CLUSTER_BAZ_BOOTSTRAP);
        });
        assertDeploymentBecomesReady(proxy);

        await().untilAsserted(() -> {
            assertClusterServiceExists(proxy, CLUSTER_FOO);
            assertClusterServiceExists(proxy, CLUSTER_BAR);
            assertClusterServiceExists(proxy, CLUSTER_BAZ);
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
        assertDeploymentBecomesReady(proxy);

        await().untilAsserted(() -> {
            var service = extension.get(Service.class, CLUSTER_FOO);
            assertThat(service)
                    .describedAs("Expect Service for cluster 'foo' to have been deleted")
                    .isNull();
        });

        await().untilAsserted(() -> {
            assertClusterServiceExists(proxy, CLUSTER_BAR);
        });
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void moveVirtualKafkaClusterToAnotherKafkaProxy() {
        // given
        KafkaProxy proxyA = extension.create(kafkaProxy(PROXY_A));
        KafkaProxy proxyB = extension.create(kafkaProxy(PROXY_B));

        KafkaClusterRef fooClusterRef = extension.create(clusterRef(CLUSTER_FOO_REF, CLUSTER_FOO_BOOTSTRAP));
        KafkaClusterRef barClusterRef = extension.create(clusterRef(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP));
        KafkaClusterRef bazClusterRef = extension.create(clusterRef(CLUSTER_BAZ_REF, CLUSTER_BAZ_BOOTSTRAP));

        extension.create(virtualKafkaCluster(CLUSTER_FOO, proxyA, fooClusterRef));
        extension.create(virtualKafkaCluster(CLUSTER_BAZ, proxyB, bazClusterRef));
        VirtualKafkaCluster barCluster = extension.create(virtualKafkaCluster(CLUSTER_BAR, proxyA, barClusterRef));

        assertProxyConfigContents(proxyA, Set.of(CLUSTER_FOO_BOOTSTRAP, CLUSTER_BAR_BOOTSTRAP), Set.of());
        assertProxyConfigContents(proxyB, Set.of(CLUSTER_BAZ_BOOTSTRAP), Set.of());
        assertClusterServiceExists(proxyA, CLUSTER_FOO);
        assertClusterServiceExists(proxyA, CLUSTER_BAR);
        assertClusterServiceExists(proxyB, CLUSTER_BAZ);

        // when
        var updatedBarCluster = new VirtualKafkaClusterBuilder(barCluster).editSpec().editProxyRef().withName(proxyB.getMetadata().getName()).endProxyRef().endSpec()
                .build();
        extension.replace(updatedBarCluster);

        // then
        assertDeploymentBecomesReady(proxyA);
        assertDeploymentBecomesReady(proxyB);
        Set<String> doesNotContain = Set.of(CLUSTER_BAR_BOOTSTRAP);
        assertProxyConfigContents(proxyA, Set.of(CLUSTER_FOO_BOOTSTRAP), doesNotContain);
        assertProxyConfigContents(proxyB, Set.of(CLUSTER_BAZ_BOOTSTRAP, CLUSTER_BAR_BOOTSTRAP), Set.of());
        assertClusterServiceExists(proxyA, CLUSTER_FOO);
        assertClusterServiceExists(proxyB, CLUSTER_BAR);
        assertClusterServiceExists(proxyB, CLUSTER_BAZ);
    }

    private static VirtualKafkaCluster virtualKafkaCluster(String clusterName, KafkaProxy proxy, KafkaClusterRef clusterRef) {
        return new VirtualKafkaClusterBuilder().withNewMetadata().withName(clusterName).endMetadata()
                .withNewSpec()
                .withNewTargetCluster()
                .withClusterRef(new ClusterRefBuilder().withName(clusterRef.getMetadata().getName()).build())
                .endTargetCluster()
                .withNewProxyRef().withName(proxy.getMetadata().getName()).endProxyRef()
                .withFilters()
                .endSpec().build();
    }

    private static KafkaClusterRef clusterRef(String clusterRefName, String clusterBootstrap) {
        return new KafkaClusterRefBuilder().withNewMetadata().withName(clusterRefName).endMetadata()
                .withNewSpec()
                .withBootstrapServers(clusterBootstrap)
                .endSpec().build();
    }

    private static Map<String, String> decodeSecretData(Secret cm) {
        return cm.getData().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new String(Base64.getDecoder().decode(entry.getValue()))));
    }

    KafkaProxy kafkaProxy(String name) {
        // @formatter:off
        return new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .build();
        // @formatter:on
    }

    private void assertClusterServiceExists(KafkaProxy proxy, String clusterName) {
        await().alias("Service as expected").untilAsserted(() -> {
            var service = extension.get(Service.class, clusterName);
            assertThat(service)
                    .describedAs("Expect Service for cluster '" + clusterName + "' to still exist")
                    .isNotNull()
                    .extracting(svc -> svc.getSpec().getSelector())
                    .describedAs("Service's selector should select proxy pods")
                    .isEqualTo(ProxyDeployment.podLabels(proxy));
        });
    }

}

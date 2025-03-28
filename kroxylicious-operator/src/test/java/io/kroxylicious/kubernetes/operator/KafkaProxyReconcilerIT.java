/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.readiness.Readiness;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.common.KafkaServiceRef;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP.Protocol.TCP;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.findOnlyResourceNamed;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaProxyReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyReconcilerIT.class);

    private static final String PROXY_A = "proxy-a";
    private static final String PROXY_B = "proxy-b";
    private static final String CLUSTER_FOO_REF = "fooref";
    private static final String FILTER_NAME = "validation";
    private static final String CLUSTER_FOO = "foo";
    private static final String CLUSTER_FOO_CLUSTERIP_INGRESS = "foo-cluster-ip";
    private static final String CLUSTER_FOO_BOOTSTRAP = "my-cluster-kafka-bootstrap.foo.svc.cluster.local:9092";
    private static final String CLUSTER_BAR_REF = "barref";
    private static final String CLUSTER_BAR = "bar";
    private static final String CLUSTER_BAR_CLUSTERIP_INGRESS = "bar-cluster-ip";
    private static final String CLUSTER_BAR_BOOTSTRAP = "my-cluster-kafka-bootstrap.bar.svc.cluster.local:9092";
    private static final String NEW_BOOTSTRAP = "new-bootstrap:9092";

    private final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    // the initial operator image pull can take a long time and interfere with the tests
    @BeforeAll
    public static void preloadOperandImage() {
        OperatorTestUtils.preloadOperandImage();
    }

    @RegisterExtension
    LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler("install", "*.ClusterRole.*.yaml");

    @RegisterExtension
    @SuppressWarnings("JUnitMalformedDeclaration") // The beforeAll and beforeEach have the same effect so we can use it as an instance field.
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new KafkaProxyReconciler(SecureConfigInterpolator.DEFAULT_INTERPOLATOR))
            .withKubernetesClient(rbacHandler.operatorClient())
            .withAdditionalCustomResourceDefinition(VirtualKafkaCluster.class)
            .withAdditionalCustomResourceDefinition(KafkaService.class)
            .withAdditionalCustomResourceDefinition(KafkaProxyIngress.class)
            .withAdditionalCustomResourceDefinition(KafkaProtocolFilter.class)
            .waitForNamespaceDeletion(true)
            .withConfigurationService(x -> x.withCloseClientOnStop(false))
            .build();
    private final LocallyRunningOperatorRbacHandler.TestActor testActor = rbacHandler.testActor(extension);

    @AfterEach
    void stopOperator() throws Exception {
        extension.getOperator().stop();
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void testCreate() {
        doCreate();
    }

    private record CreatedResources(KafkaProxy proxy, Set<VirtualKafkaCluster> clusters, Set<KafkaService> clusterRefs, Set<KafkaProxyIngress> ingresses) {
        VirtualKafkaCluster cluster(String name) {
            return findOnlyResourceNamed(name, clusters).orElseThrow();
        }

        KafkaService clusterRef(String name) {
            return findOnlyResourceNamed(name, clusterRefs).orElseThrow();
        }

        KafkaProxyIngress ingress(String name) {
            return findOnlyResourceNamed(name, ingresses).orElseThrow();
        }
    }

    CreatedResources doCreate() {
        KafkaProxy proxy = testActor.create(kafkaProxy(PROXY_A));
        KafkaProtocolFilter filter = testActor.create(filter(FILTER_NAME));
        KafkaService barClusterRef = testActor.create(clusterRef(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP));
        KafkaProxyIngress ingressBar = testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, proxy));
        Set<KafkaService> clusterRefs = Set.of(barClusterRef);
        VirtualKafkaCluster clusterBar = testActor.create(virtualKafkaCluster(CLUSTER_BAR, proxy, barClusterRef, ingressBar, filter));
        Set<VirtualKafkaCluster> clusters = Set.of(clusterBar);
        assertProxyConfigContents(proxy, Set.of(CLUSTER_BAR_BOOTSTRAP, filter.getSpec().getType()), Set.of());
        assertDeploymentMountsConfigConfigMap(proxy);
        assertDeploymentBecomesReady(proxy);
        assertServiceTargetsProxyInstances(proxy, clusterBar, ingressBar);
        return new CreatedResources(proxy, clusters, clusterRefs, Set.of(ingressBar));
    }

    private KafkaProxyIngress clusterIpIngress(String ingressName, KafkaProxy proxy) {
        return new KafkaProxyIngressBuilder().withNewMetadata().withName(ingressName).endMetadata()
                .withNewSpec().withNewClusterIP().withProtocol(TCP).endClusterIP().withNewProxyRef().withName(name(proxy)).endProxyRef().endSpec()
                .build();
    }

    private void assertDeploymentBecomesReady(KafkaProxy proxy) {
        // wait longer for initial operator image download
        AWAIT.alias("Deployment as expected").untilAsserted(() -> {
            var deployment = testActor.get(Deployment.class, ProxyDeployment.deploymentName(proxy));
            assertThat(deployment)
                    .describedAs("All deployment replicas should become ready")
                    .returns(true, Readiness::isDeploymentReady);
        });
    }

    private void assertServiceTargetsProxyInstances(KafkaProxy proxy, VirtualKafkaCluster cluster, KafkaProxyIngress ingress) {
        AWAIT.alias("cluster Services as expected").untilAsserted(() -> {
            String clusterName = name(cluster);
            String ingressName = name(ingress);
            String serviceName = clusterName + "-" + ingressName;
            var service = testActor.get(Service.class, serviceName);
            assertThat(service).isNotNull()
                    .describedAs(
                            "Expect Service for cluster '" + clusterName + "' and ingress '" + ingressName + "' to still exist")
                    .extracting(svc -> svc.getSpec().getSelector())
                    .describedAs("Service's selector should select proxy pods")
                    .isEqualTo(ProxyDeployment.podLabels(proxy));
        });
    }

    private void assertDeploymentMountsConfigConfigMap(KafkaProxy proxy) {
        AWAIT.alias("Deployment as expected").untilAsserted(() -> {
            var deployment = testActor.get(Deployment.class, ProxyDeployment.deploymentName(proxy));
            assertThat(deployment).isNotNull()
                    .extracting(dep -> dep.getSpec().getTemplate().getSpec().getVolumes(), InstanceOfAssertFactories.list(Volume.class))
                    .describedAs("Deployment template should mount the proxy config configmap")
                    .filteredOn(volume -> volume.getConfigMap() != null)
                    .map(Volume::getConfigMap)
                    .anyMatch(cm -> cm.getName().equals(ProxyConfigConfigMap.configMapName(proxy)));
        });
    }

    private void assertProxyConfigContents(KafkaProxy cr, Set<String> contains, Set<String> notContains) {
        AWAIT.alias("Config as expected").untilAsserted(() -> {
            AbstractStringAssert<?> proxyConfig = assertThatProxyConfigFor(cr);
            if (!contains.isEmpty()) {
                proxyConfig.contains(contains);
            }
            if (!notContains.isEmpty()) {
                proxyConfig.doesNotContain(notContains);
            }
        });
    }

    @Test
    void testDelete() {
        var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;
        testActor.delete(proxy);

        AWAIT.alias("ConfigMap was deleted").untilAsserted(() -> {
            var configMap = testActor.get(ConfigMap.class, ProxyConfigConfigMap.configMapName(proxy));
            assertThat(configMap).isNull();
        });
        AWAIT.alias("Deployment was deleted").untilAsserted(() -> {
            var deployment = testActor.get(Deployment.class, ProxyDeployment.deploymentName(proxy));
            assertThat(deployment).isNull();
        });
        AWAIT.alias("Services were deleted").untilAsserted(() -> {
            for (var cluster : createdResources.clusters) {
                var service = testActor.get(Service.class, ClusterService.serviceName(cluster));
                assertThat(service).isNull();
            }
        });
    }

    @Test
    void testUpdateVirtualClusterTargetBootstrap() {
        final var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;
        var clusterRef = createdResources.clusterRef(CLUSTER_BAR_REF).edit().editSpec().withBootstrapServers(NEW_BOOTSTRAP).endSpec().build();
        testActor.replace(clusterRef);

        assertDeploymentBecomesReady(proxy);
        AWAIT.untilAsserted(() -> {
            assertThatProxyConfigFor(proxy)
                    .doesNotContain(CLUSTER_BAR_BOOTSTRAP)
                    .contains(NEW_BOOTSTRAP);
        });

        assertServiceTargetsProxyInstances(proxy, createdResources.cluster(CLUSTER_BAR), createdResources.ingress(CLUSTER_BAR_CLUSTERIP_INGRESS));
    }

    @Test
    void testUpdateVirtualClusterClusterRef() {
        // given
        final var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;

        String newClusterRefName = "new-cluster-ref";
        testActor.create(clusterRef(newClusterRefName, NEW_BOOTSTRAP));

        KafkaServiceRef newClusterRef = new KafkaServiceRefBuilder().withName(newClusterRefName).build();
        var cluster = createdResources.cluster(CLUSTER_BAR).edit().editSpec().withTargetKafkaServiceRef(newClusterRef).endSpec().build();

        // when
        testActor.replace(cluster);

        // then
        assertDeploymentBecomesReady(proxy);
        AWAIT.untilAsserted(() -> assertThatProxyConfigFor(proxy)
                .doesNotContain(CLUSTER_BAR_BOOTSTRAP)
                .contains(NEW_BOOTSTRAP));

        assertServiceTargetsProxyInstances(proxy, createdResources.cluster(CLUSTER_BAR), createdResources.ingress(CLUSTER_BAR_CLUSTERIP_INGRESS));
    }

    @Test
    void testDeleteVirtualCluster() {
        final var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;
        testActor.delete(createdResources.cluster(CLUSTER_BAR));

        AWAIT.untilAsserted(() -> assertThatProxyConfigFor(proxy).doesNotContain(CLUSTER_BAR_BOOTSTRAP));
        AWAIT.untilAsserted(() -> {
            var service = testActor.get(Service.class, CLUSTER_BAR);
            assertThat(service)
                    .describedAs("Expect Service for cluster 'bar' to have been deleted")
                    .isNull();
        });
    }

    @Test
    void moveVirtualKafkaClusterToAnotherKafkaProxy() {
        // given
        KafkaProxy proxyA = testActor.create(kafkaProxy(PROXY_A));
        KafkaProxy proxyB = testActor.create(kafkaProxy(PROXY_B));
        KafkaProxyIngress ingressFoo = testActor.create(clusterIpIngress(CLUSTER_FOO_CLUSTERIP_INGRESS, proxyA));
        KafkaProxyIngress ingressBar = testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, proxyB));

        KafkaService fooClusterRef = testActor.create(clusterRef(CLUSTER_FOO_REF, CLUSTER_FOO_BOOTSTRAP));
        KafkaService barClusterRef = testActor.create(clusterRef(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP));
        KafkaProtocolFilter filter = testActor.create(filter(FILTER_NAME));

        VirtualKafkaCluster clusterFoo = testActor.create(virtualKafkaCluster(CLUSTER_FOO, proxyA, fooClusterRef, ingressFoo, filter));
        VirtualKafkaCluster barCluster = testActor.create(virtualKafkaCluster(CLUSTER_BAR, proxyB, barClusterRef, ingressBar, filter));

        assertProxyConfigContents(proxyA, Set.of(CLUSTER_FOO_BOOTSTRAP), Set.of());
        assertProxyConfigContents(proxyB, Set.of(CLUSTER_BAR_BOOTSTRAP), Set.of());
        assertServiceTargetsProxyInstances(proxyA, clusterFoo, ingressFoo);
        assertServiceTargetsProxyInstances(proxyB, barCluster, ingressBar);

        // must swap ingresses so both proxy instances have a single port-per-broker ingress
        testActor.replace(clusterIpIngress(CLUSTER_FOO_CLUSTERIP_INGRESS, proxyB));
        testActor.replace(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, proxyA));
        var updatedFooCluster = new VirtualKafkaClusterBuilder(clusterFoo).editSpec().editProxyRef().withName(name(proxyB)).endProxyRef().endSpec()
                .build();
        testActor.replace(updatedFooCluster);
        var updatedBarCluster = new VirtualKafkaClusterBuilder(barCluster).editSpec().editProxyRef().withName(name(proxyA)).endProxyRef().endSpec()
                .build();
        testActor.replace(updatedBarCluster);

        // then
        assertDeploymentBecomesReady(proxyA);
        assertDeploymentBecomesReady(proxyB);
        assertProxyConfigContents(proxyA, Set.of(CLUSTER_BAR_BOOTSTRAP), Set.of(CLUSTER_FOO_BOOTSTRAP));
        assertProxyConfigContents(proxyB, Set.of(CLUSTER_FOO_BOOTSTRAP), Set.of(CLUSTER_BAR_BOOTSTRAP));
        assertServiceTargetsProxyInstances(proxyA, barCluster, ingressBar);
        assertServiceTargetsProxyInstances(proxyB, clusterFoo, ingressFoo);
    }

    private AbstractStringAssert<?> assertThatProxyConfigFor(KafkaProxy proxy) {
        var configMap = testActor.get(ConfigMap.class, ProxyConfigConfigMap.configMapName(proxy));
        return assertThat(configMap)
                .isNotNull()
                .extracting(ConfigMap::getData, InstanceOfAssertFactories.map(String.class, String.class))
                .containsKey(ProxyConfigConfigMap.CONFIG_YAML_KEY)
                .extracting(map -> map.get(ProxyConfigConfigMap.CONFIG_YAML_KEY), InstanceOfAssertFactories.STRING);
    }

    private static VirtualKafkaCluster virtualKafkaCluster(String clusterName, KafkaProxy proxy, KafkaService clusterRef,
                                                           KafkaProxyIngress ingress, KafkaProtocolFilter filter) {
        return new VirtualKafkaClusterBuilder().withNewMetadata().withName(clusterName).endMetadata()
                .withNewSpec()
                .withTargetKafkaServiceRef(new KafkaServiceRefBuilder().withName(name(clusterRef)).build())
                .withNewProxyRef().withName(name(proxy)).endProxyRef()
                .addNewIngressRef().withName(name(ingress)).endIngressRef()
                .withFilterRefs().addNewFilterRef().withName(name(filter)).endFilterRef()
                .endSpec().build();
    }

    private static KafkaService clusterRef(String clusterRefName, String clusterBootstrap) {
        return new KafkaServiceBuilder().withNewMetadata().withName(clusterRefName).endMetadata()
                .withNewSpec()
                .withBootstrapServers(clusterBootstrap)
                .endSpec().build();
    }

    private static KafkaProtocolFilter filter(String name) {
        return new KafkaProtocolFilterBuilder().withNewMetadata().withName(name).endMetadata()
                .withNewSpec().withType("RecordValidation").withConfigTemplate(Map.of("rules", List.of(Map.of("allowNulls", false))))
                .endSpec().build();
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

}

/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Duration;
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

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaProxyReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyReconcilerIT.class);

    private static final String CLUSTER_FOO_BOOTSTRAP = "my-cluster-kafka-bootstrap.foo.svc.cluster.local:9092";
    private static final String CLUSTER_BAR_BOOTSTRAP = "my-cluster-kafka-bootstrap.bar.svc.cluster.local:9092";
    private static final String NEW_BOOTSTRAP = "new-bootstrap:9092";

    private final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    // the initial operator image pull can take a long time and interfere with the tests
    @BeforeAll
    public static void preloadOperandImage() {
        OperatorTestUtils.preloadOperandImage();
    }

    @RegisterExtension
    static LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler("install", "*.ClusterRole.*.yaml");

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
        KafkaProxy proxy = testActor.kafkaProxy().create();
        KafkaProtocolFilter filter = testActor.protocolFilter().withArbitraryFilterConfig().create();
        KafkaService barService = testActor.kafkaService().withBootstrapServers(CLUSTER_BAR_BOOTSTRAP).create();
        KafkaProxyIngress ingressBar = testActor.ingress().withClusterIpForProxy(proxy).create();
        VirtualKafkaCluster clusterBar = testActor.virtualKafkaCluster().withProxyRef(proxy).withTargetKafkaService(barService).withFilters(filter)
                .withIngresses(ingressBar).create();
        assertProxyResourcesCreated(proxy, filter, clusterBar, ingressBar);
    }

    @Test
    void testDelete() {
        // given
        KafkaProxy proxy = testActor.kafkaProxy().create();
        KafkaProtocolFilter filter = testActor.protocolFilter().withArbitraryFilterConfig().create();
        KafkaService kafkaService = testActor.kafkaService().withBootstrapServers(CLUSTER_BAR_BOOTSTRAP).create();
        KafkaProxyIngress ingress = testActor.ingress().withClusterIpForProxy(proxy).create();
        VirtualKafkaCluster cluster = testActor.virtualKafkaCluster().withProxyRef(proxy).withTargetKafkaService(kafkaService).withFilters(filter)
                .withIngresses(ingress).create();
        assertProxyResourcesCreated(proxy, filter, cluster, ingress);

        // when
        testActor.delete(proxy);

        // then
        AWAIT.alias("ConfigMap was deleted").untilAsserted(() -> {
            var configMap = testActor.get(ConfigMap.class, ProxyConfigConfigMap.configMapName(proxy));
            assertThat(configMap).isNull();
        });
        AWAIT.alias("Deployment was deleted").untilAsserted(() -> {
            var deployment = testActor.get(Deployment.class, ProxyDeployment.deploymentName(proxy));
            assertThat(deployment).isNull();
        });
        AWAIT.alias("Services were deleted").untilAsserted(() -> {
            var service = testActor.get(Service.class, ClusterService.serviceName(cluster));
            assertThat(service).isNull();
        });
    }

    @Test
    void testUpdateVirtualClusterTargetBootstrap() {
        // given
        KafkaProxy proxy = testActor.kafkaProxy().create();
        KafkaProtocolFilter filter = testActor.protocolFilter().withArbitraryFilterConfig().create();
        KafkaService kafkaService = testActor.kafkaService().withBootstrapServers(CLUSTER_BAR_BOOTSTRAP).create();
        KafkaProxyIngress ingress = testActor.ingress().withClusterIpForProxy(proxy).create();
        VirtualKafkaCluster cluster = testActor.virtualKafkaCluster().withProxyRef(proxy).withTargetKafkaService(kafkaService).withFilters(filter)
                .withIngresses(ingress).create();
        assertProxyResourcesCreated(proxy, filter, cluster, ingress);

        // when
        testActor.kafkaService(name(kafkaService)).withBootstrapServers(NEW_BOOTSTRAP).replace();

        // then
        assertDeploymentBecomesReady(proxy);
        AWAIT.untilAsserted(() -> {
            assertThatProxyConfigFor(proxy)
                    .doesNotContain(CLUSTER_BAR_BOOTSTRAP)
                    .contains(NEW_BOOTSTRAP);
        });

        assertServiceTargetsProxyInstances(proxy, cluster, ingress);
    }

    @Test
    void testUpdateVirtualClusterClusterRef() {
        // given
        KafkaProxy proxy = testActor.kafkaProxy().create();
        KafkaProtocolFilter filter = testActor.protocolFilter().withArbitraryFilterConfig().create();
        KafkaService barService = testActor.kafkaService().withBootstrapServers(CLUSTER_BAR_BOOTSTRAP).create();
        KafkaProxyIngress ingressBar = testActor.ingress().withClusterIpForProxy(proxy).create();
        VirtualKafkaCluster clusterBar = testActor.virtualKafkaCluster().withProxyRef(proxy).withTargetKafkaService(barService).withFilters(filter)
                .withIngresses(ingressBar).create();

        assertProxyConfigContents(proxy, Set.of(CLUSTER_BAR_BOOTSTRAP, filter.getSpec().getType()), Set.of());
        assertDeploymentMountsConfigConfigMap(proxy);
        assertDeploymentBecomesReady(proxy);
        assertServiceTargetsProxyInstances(proxy, clusterBar, ingressBar);

        KafkaService kafkaService = testActor.kafkaService().withBootstrapServers(NEW_BOOTSTRAP).create();

        var cluster = testActor.virtualKafkaCluster(name(clusterBar)).withTargetKafkaService(kafkaService).replace();

        // when
        testActor.replace(cluster);

        // then
        assertDeploymentBecomesReady(proxy);
        AWAIT.untilAsserted(() -> assertThatProxyConfigFor(proxy)
                .doesNotContain(CLUSTER_BAR_BOOTSTRAP)
                .contains(NEW_BOOTSTRAP));

        assertServiceTargetsProxyInstances(proxy, clusterBar, ingressBar);
    }

    @Test
    void testDeleteVirtualCluster() {
        // given
        KafkaProxy proxy = testActor.kafkaProxy().create();
        KafkaProtocolFilter filter = testActor.protocolFilter().withArbitraryFilterConfig().create();
        KafkaService kafkaService = testActor.kafkaService().withBootstrapServers(CLUSTER_BAR_BOOTSTRAP).create();
        KafkaProxyIngress ingress = testActor.ingress().withClusterIpForProxy(proxy).create();
        VirtualKafkaCluster cluster = testActor.virtualKafkaCluster().withProxyRef(proxy).withTargetKafkaService(kafkaService).withFilters(filter)
                .withIngresses(ingress).create();
        assertProxyResourcesCreated(proxy, filter, cluster, ingress);

        // when
        testActor.delete(cluster);

        // then
        AWAIT.untilAsserted(() -> assertThatProxyConfigFor(proxy).doesNotContain(CLUSTER_BAR_BOOTSTRAP));
        AWAIT.untilAsserted(() -> {
            String clusterName = name(cluster);
            var service = testActor.get(Service.class, clusterName);
            assertThat(service)
                    .describedAs("Expect Service for cluster '" + clusterName + "' to have been deleted")
                    .isNull();
        });
    }

    @Test
    void moveVirtualKafkaClusterToAnotherKafkaProxy() {
        // given
        KafkaProxy proxyA = testActor.kafkaProxy().create();
        KafkaProxy proxyB = testActor.kafkaProxy().create();
        KafkaProxyIngress ingressFoo = testActor.ingress().withClusterIpForProxy(proxyA).create();
        KafkaProxyIngress ingressBar = testActor.ingress().withClusterIpForProxy(proxyB).create();

        KafkaService fooClusterRef = testActor.kafkaService().withBootstrapServers(CLUSTER_FOO_BOOTSTRAP).create();
        KafkaService barClusterRef = testActor.kafkaService().withBootstrapServers(CLUSTER_BAR_BOOTSTRAP).create();
        KafkaProtocolFilter filter = testActor.protocolFilter().withArbitraryFilterConfig().create();

        VirtualKafkaCluster clusterFoo = testActor.virtualKafkaCluster().withProxyRef(proxyA).withTargetKafkaService(fooClusterRef).withFilters(filter)
                .withIngresses(ingressFoo).create();

        VirtualKafkaCluster barCluster = testActor.virtualKafkaCluster().withProxyRef(proxyB).withTargetKafkaService(barClusterRef).withFilters(filter)
                .withIngresses(ingressBar).create();

        assertProxyConfigContents(proxyA, Set.of(CLUSTER_FOO_BOOTSTRAP), Set.of());
        assertProxyConfigContents(proxyB, Set.of(CLUSTER_BAR_BOOTSTRAP), Set.of());
        assertServiceTargetsProxyInstances(proxyA, clusterFoo, ingressFoo);
        assertServiceTargetsProxyInstances(proxyB, barCluster, ingressBar);

        // must swap ingresses so both proxy instances have a single port-per-broker ingress
        testActor.ingress(name(ingressFoo)).withClusterIpForProxy(proxyB).replace();
        testActor.ingress(name(ingressBar)).withClusterIpForProxy(proxyA).replace();
        testActor.virtualKafkaCluster(name(clusterFoo)).withProxyRef(proxyB).replace();
        testActor.virtualKafkaCluster(name(barCluster)).withProxyRef(proxyA).replace();

        // then
        assertDeploymentBecomesReady(proxyA);
        assertDeploymentBecomesReady(proxyB);
        assertProxyConfigContents(proxyA, Set.of(CLUSTER_BAR_BOOTSTRAP), Set.of(CLUSTER_FOO_BOOTSTRAP));
        assertProxyConfigContents(proxyB, Set.of(CLUSTER_FOO_BOOTSTRAP), Set.of(CLUSTER_BAR_BOOTSTRAP));
        assertServiceTargetsProxyInstances(proxyA, barCluster, ingressBar);
        assertServiceTargetsProxyInstances(proxyB, clusterFoo, ingressFoo);
    }

    private void assertProxyResourcesCreated(KafkaProxy proxy, KafkaProtocolFilter filter, VirtualKafkaCluster clusterBar, KafkaProxyIngress ingressBar) {
        assertProxyConfigContents(proxy, Set.of(CLUSTER_BAR_BOOTSTRAP, filter.getSpec().getType()), Set.of());
        assertDeploymentMountsConfigConfigMap(proxy);
        assertDeploymentBecomesReady(proxy);
        assertServiceTargetsProxyInstances(proxy, clusterBar, ingressBar);
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

    private AbstractStringAssert<?> assertThatProxyConfigFor(KafkaProxy proxy) {
        var configMap = testActor.get(ConfigMap.class, ProxyConfigConfigMap.configMapName(proxy));
        return assertThat(configMap)
                .isNotNull()
                .extracting(ConfigMap::getData, InstanceOfAssertFactories.map(String.class, String.class))
                .containsKey(ProxyConfigConfigMap.CONFIG_YAML_KEY)
                .extracting(map -> map.get(ProxyConfigConfigMap.CONFIG_YAML_KEY), InstanceOfAssertFactories.STRING);
    }

}

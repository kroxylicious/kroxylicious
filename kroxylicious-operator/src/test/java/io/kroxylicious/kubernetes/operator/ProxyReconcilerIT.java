/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
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
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ProxyReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyReconcilerIT.class);

    public static final String RESOURCE_NAME = "test-proxy";
    public static final String CLUSTER_FOO = "foo";
    public static final String CLUSTER_FOO_BOOTSTRAP = "my-cluster-kafka-bootstrap.foo.svc.cluster.local:9092";
    public static final String CLUSTER_BAR = "bar";
    public static final String CLUSTER_BAR_BOOTSTRAP = "my-cluster-kafka-bootstrap.bar.svc.cluster.local:9092";

    static KubernetesClient client;

    @BeforeAll
    static void checkKubeAvailable() {
        boolean haveKube;
        client = new KubernetesClientBuilder().build();
        try {
            client.namespaces().list();
            haveKube = true;
        }
        catch (KubernetesClientException e) {
            haveKube = false;
            client.close();
        }
        Assumptions.assumeTrue(haveKube, "Test requires a viable kube client");
    }

    @RegisterExtension
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new ProxyReconciler(new RuntimeDecl(List.of(
                    //new FilterKindDecl("filter.kroxylicious.io", "v1alpha1", "RecordEncryption", "io.kroxylicious.filter.encryption.RecordEncryption")
                    ))))
            .withKubernetesClient(client)
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

    KafkaProxy doCreate() {
        final var cr = extension.create(testResource());

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
            for (var cluster : cr.getSpec().getClusters()) {
                var service = extension.get(Service.class, ClusterService.serviceName(cluster));
                assertThat(service).isNotNull()
                        .extracting(svc -> svc.getSpec().getSelector())
                        .describedAs("Service's selector should select proxy pods")
                        .isEqualTo(ProxyDeployment.podLabels());
            }
        });

        return cr;
    }

    @Test
    void testDelete() {
        var cr = doCreate();
        extension.delete(cr);

        await().alias("Secret was deleted").untilAsserted(() -> {
            var secret = extension.get(Secret.class, ProxyConfigSecret.secretName(cr));
            assertThat(secret).isNull();
        });
        await().alias("Deployment was deleted").untilAsserted(() -> {
            var deployment = extension.get(Deployment.class, ProxyDeployment.deploymentName(cr));
            assertThat(deployment).isNull();
        });
        await().alias("Services were deleted").untilAsserted(() -> {
            for (var cluster : cr.getSpec().getClusters()) {
                var service = extension.get(Service.class, ClusterService.serviceName(cluster));
                assertThat(service).isNull();
            }
        });
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void testUpdate() {
        final var cr = doCreate();
        // @formatter:off
        var changedCr = new KafkaProxyBuilder(cr)
                .editSpec()
                    .removeMatchingFromClusters(cluster -> CLUSTER_FOO.equals(cluster.getName()))
                .endSpec()
                .build();
        // @formatter:on
        extension.replace(changedCr);

        await().untilAsserted(() -> {
            var secret = extension.get(Secret.class, ProxyConfigSecret.secretName(cr));
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
            var service = extension.get(Service.class, CLUSTER_BAR);
            assertThat(service)
                    .describedAs("Expect Service for cluster 'bar' to still exist")
                    .isNotNull()
                    .extracting(svc -> svc.getSpec().getSelector())
                    .describedAs("Service's selector should select proxy pods")
                    .isEqualTo(ProxyDeployment.podLabels());
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
                .withNewSpec()
                    .addNewCluster()
                        .withName(CLUSTER_FOO)
                        .withNewUpstream()
                            .withBootstrapServers(CLUSTER_FOO_BOOTSTRAP)
                        .endUpstream()
                    .endCluster()
                    .addNewCluster()
                        .withName(CLUSTER_BAR)
                        .withNewUpstream()
                            .withBootstrapServers(CLUSTER_BAR_BOOTSTRAP)
                        .endUpstream()
                    .endCluster()
                .endSpec()
                .build();
        // @formatter:on
    }
}

/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.readiness.Readiness;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Workflow;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.BooleanWithUndefined;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.common.IngressRefBuilder;
import io.kroxylicious.kubernetes.api.common.Protocol;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatusBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatusBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatusBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.operator.LocallyRunningOperatorRbacHandler;
import io.kroxylicious.kubernetes.operator.OperatorTestUtils;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.SecureConfigInterpolator;
import io.kroxylicious.kubernetes.operator.TestFiles;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.generation;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that upgrading from the pre-SSA operator to the SSA operator
 * preserves externally-applied SSA patches on the proxy Deployment.
 * <p>
 * This test has a short shelf life: once all users have migrated past the
 * non-SSA operator, the upgrade scenario no longer applies.
 * <p>
 * Not migrated to {@link io.kroxylicious.kubernetes.operator.LocalKroxyliciousOperatorExtension}:
 * this test requires a dual-operator lifecycle (two operator instances started and stopped
 * independently within a single test) which the extension does not support.
 */
@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class OperatorSsaUpgradeIT {

    private static final String PROXY_NAME = "proxy-a";
    private static final String CLUSTER_NAME = "bar";
    private static final String SERVICE_REF = "barref";
    private static final String INGRESS_NAME = "bar-cluster-ip";
    private static final String BOOTSTRAP = "my-cluster-kafka-bootstrap.bar.svc.cluster.local:9092";

    @BeforeAll
    static void preloadOperandImage() {
        String image = ProxyDeploymentDependentResource.getOperandImage();
        try (var client = OperatorTestUtils.kubeClient()) {
            var pod = client.run().withName("preload-operand-image")
                    .withNewRunConfig()
                    .withImage(image)
                    .withRestartPolicy("Never")
                    .withCommand("ls").done();
            try {
                client.resource(pod).waitUntilCondition(Readiness::isPodSucceeded, 2, TimeUnit.MINUTES);
            }
            finally {
                client.resource(pod).delete();
            }
        }
    }

    @RegisterExtension
    static LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler(TestFiles.INSTALL_MANIFESTS_DIR, "*.ClusterRole.*.yaml");

    @RegisterExtension
    @SuppressWarnings("JUnitMalformedDeclaration")
    LocallyRunOperatorExtension legacyExtension = LocallyRunOperatorExtension.builder()
            .withReconciler(new LegacyDeploymentReconciler())
            .withKubernetesClient(rbacHandler.operatorClient())
            .withAdditionalCustomResourceDefinition(VirtualKafkaCluster.class)
            .withAdditionalCustomResourceDefinition(KafkaService.class)
            .withAdditionalCustomResourceDefinition(KafkaProxyIngress.class)
            .withAdditionalCustomResourceDefinition(KafkaProtocolFilter.class)
            .waitForNamespaceDeletion(false)
            .withConfigurationService(x -> x.withCloseClientOnStop(false))
            .build();

    private final LocallyRunningOperatorRbacHandler.TestActor testActor = rbacHandler.testActor(legacyExtension);

    @AfterEach
    void stopLegacyOperator() {
        legacyExtension.getOperator().stop();
    }

    @Test
    void externalSsaPatchOnDeploymentSurvivesUpgradeFromNonSsaToSsaOperator() {
        String namespace = legacyExtension.getNamespace();

        // Given — legacy (non-SSA) operator creates the Deployment via PUT
        testActor.create(new KafkaProxyBuilder()
                .withNewMetadata().withName(PROXY_NAME).endMetadata()
                .withNewSpec().withReplicas(1).endSpec()
                .build());

        await().alias("Deployment created by legacy operator")
                .atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    var deployment = testActor.get(Deployment.class, PROXY_NAME);
                    assertThat(deployment).isNotNull();
                    assertThat(deployment.getMetadata().getManagedFields())
                            .as("Deployment should have been created via non-SSA (operation: Update)")
                            .anyMatch(mf -> "Update".equals(mf.getOperation()));
                });

        // Stop the legacy operator before starting the SSA operator
        legacyExtension.getOperator().stop();

        // Create the cluster resources needed for the SSA operator to reconcile the Deployment
        KafkaProxyIngress ingress = createIngressWithStatus();
        createServiceWithStatus();
        createClusterWithStatus(name(ingress));

        // Apply an external SSA annotation before the SSA operator starts
        try (var client = new KubernetesClientBuilder().build()) {
            client.resource(new DeploymentBuilder()
                    .withNewMetadata().withName(PROXY_NAME).withNamespace(namespace)
                    .addToAnnotations("test.kroxylicious.io/external", "present")
                    .endMetadata()
                    .build())
                    .fieldManager("test-external-tool")
                    .serverSideApply();
        }

        // When — upgrade: start the SSA-enabled operator in the same namespace
        var ssaOperator = new Operator(o -> o.withKubernetesClient(rbacHandler.operatorClient()).withCloseClientOnStop(false));
        ssaOperator.register(
                new KafkaProxyReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR),
                o -> o.settingNamespaces(namespace));
        ssaOperator.start();
        try {
            // Trigger a reconcile by bumping the replica count
            KafkaProxy current = Objects.requireNonNull(testActor.get(KafkaProxy.class, PROXY_NAME));
            testActor.replace(current.edit().editSpec().withReplicas(2).endSpec().build());

            // Then — replica count changes (proves the SSA operator has reconciled the Deployment)
            await().alias("Deployment replica count updated by SSA operator")
                    .atMost(Duration.ofSeconds(60))
                    .untilAsserted(() -> {
                        var deployment = testActor.get(Deployment.class, PROXY_NAME);
                        assertThat(deployment).isNotNull();
                        assertThat(deployment.getSpec().getReplicas()).isEqualTo(2);
                    });

            // Then — the external annotation must still be present
            assertThat(testActor.get(Deployment.class, PROXY_NAME))
                    .isNotNull()
                    .extracting(r -> r.getMetadata().getAnnotations(),
                            InstanceOfAssertFactories.map(String.class, String.class))
                    .as("External SSA annotation should survive operator upgrade")
                    .containsEntry("test.kroxylicious.io/external", "present");
        }
        finally {
            ssaOperator.stop();
        }
    }

    private KafkaProxyIngress createIngressWithStatus() {
        KafkaProxyIngress ingress = testActor.create(new KafkaProxyIngressBuilder()
                .withNewMetadata().withName(OperatorSsaUpgradeIT.INGRESS_NAME).endMetadata()
                .withNewSpec()
                .withNewClusterIP().withProtocol(Protocol.TCP).endClusterIP()
                .withNewProxyRef().withName(PROXY_NAME).endProxyRef()
                .endSpec()
                .build());
        ingress.setStatus(new KafkaProxyIngressStatusBuilder().withObservedGeneration(generation(ingress)).build());
        return testActor.patchStatus(ingress);
    }

    private void createServiceWithStatus() {
        KafkaService service = testActor.create(new KafkaServiceBuilder()
                .withNewMetadata().withName(OperatorSsaUpgradeIT.SERVICE_REF).endMetadata()
                .withNewSpec().withBootstrapServers(OperatorSsaUpgradeIT.BOOTSTRAP).endSpec()
                .build());
        service.setStatus(new KafkaServiceStatusBuilder().withObservedGeneration(generation(service)).withBootstrapServers(OperatorSsaUpgradeIT.BOOTSTRAP).build());
        testActor.patchStatus(service);
    }

    private void createClusterWithStatus(String ingressName) {
        VirtualKafkaCluster cluster = testActor.create(new VirtualKafkaClusterBuilder()
                .withNewMetadata().withName(OperatorSsaUpgradeIT.CLUSTER_NAME).endMetadata()
                .withNewSpec()
                .withNewProxyRef().withName(PROXY_NAME).endProxyRef()
                .withNewTargetKafkaServiceRef().withName(OperatorSsaUpgradeIT.SERVICE_REF).endTargetKafkaServiceRef()
                .withIngresses(List.of(new IngressesBuilder()
                        .withIngressRef(new IngressRefBuilder().withName(ingressName).build())
                        .build()))
                .endSpec()
                .build());
        cluster.setStatus(new VirtualKafkaClusterStatusBuilder().withObservedGeneration(generation(cluster)).build());
        testActor.patchStatus(cluster);
    }

    /**
     * A minimal reconciler that creates the proxy Deployment using the old non-SSA (PUT) approach,
     * simulating the operator behaviour before SSA was enabled on the Deployment dependent resource.
     */
    @Workflow(dependents = {
            @Dependent(type = LegacyDeploymentDependentResource.class)
    })
    static class LegacyDeploymentReconciler implements Reconciler<KafkaProxy> {

        @Override
        public UpdateControl<KafkaProxy> reconcile(KafkaProxy resource, Context<KafkaProxy> context) {
            return UpdateControl.noUpdate();
        }
    }

    /**
     * Creates the proxy Deployment without SSA ({@code useSSA=FALSE}), simulating the pre-SSA operator.
     * Uses the same Deployment name and label selector as {@link ProxyDeploymentDependentResource}
     * so that the SSA operator can adopt the same resource in phase 2.
     */
    @KubernetesDependent(useSSA = BooleanWithUndefined.FALSE)
    static class LegacyDeploymentDependentResource extends CRUDKubernetesDependentResource<Deployment, KafkaProxy> {

        LegacyDeploymentDependentResource() {
            super(Deployment.class);
        }

        @Override
        public Deployment desired(KafkaProxy primary, Context<KafkaProxy> context) {
            var labels = standardLabels(primary);
            // @formatter:off
            return new DeploymentBuilder()
                    .withNewMetadata()
                        .withName(ResourcesUtil.name(primary))
                        .withNamespace(ResourcesUtil.namespace(primary))
                        .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(primary)).endOwnerReference()
                        .addToLabels(labels)
                    .endMetadata()
                    .withNewSpec()
                        .withReplicas(primary.getSpec() == null ? 1 : primary.getSpec().getReplicas())
                        .editOrNewSelector()
                            .withMatchLabels(labels)
                        .endSelector()
                        .withNewTemplate()
                            .withNewMetadata()
                                .addToLabels(labels)
                            .endMetadata()
                            .withNewSpec()
                                .addNewContainer()
                                    .withName("proxy")
                                    .withImage(ProxyDeploymentDependentResource.getOperandImage())
                                .endContainer()
                            .endSpec()
                        .endTemplate()
                    .endSpec()
                    .build();
            // @formatter:on
        }
    }
}

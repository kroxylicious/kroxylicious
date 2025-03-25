/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.operator.assertj.KafkaProtocolFilterStatusAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaProtocolFilterReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProtocolFilterReconcilerIT.class);

    private static final String A = "a";
    private static final String B = "b";
    private static final String C = "c";
    private static final String FILTER_ONE = "one";

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    // the initial operator image pull can take a long time and interfere with the tests
    @BeforeAll
    static void preloadOperandImage() {
        OperatorTestUtils.preloadOperandImage();
    }

    @RegisterExtension
    LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler(Path.of("install"), "*.ClusterRole.kroxylicious-operator-filter-generic.yaml",
            "*.ClusterRole.kroxylicious-operator-watched.yaml");

    @RegisterExtension
    @SuppressWarnings("JUnitMalformedDeclaration") // The beforeAll and beforeEach have the same effect so we can use it as an instance field.
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new KafkaProtocolFilterReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR))
            .withAdditionalCustomResourceDefinition(KafkaProtocolFilter.class)
            .withKubernetesClient(rbacHandler.operatorClient())
            .waitForNamespaceDeletion(true)
            .withConfigurationService(x -> x.withCloseClientOnStop(false))
            .build();

    private final LocallyRunningOperatorRbacHandler.TestActor testActor = rbacHandler.testActor(extension);

    @AfterEach
    void stopOperator() {
        extension.getOperator().stop();
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void shouldEventuallyResolveWhenFilterCreatedFirst() {
        createFilterFirst();
    }

    private KafkaProtocolFilter createFilterFirst() {
        KafkaProtocolFilter filterOne = testActor.create(filter(FILTER_ONE,
                "${secret:" + A + ":foo}", "${configmap:" + B + ":foo}"));
        assertStatusResolvedRefs(filterOne, Condition.Status.FALSE, "Referenced Secrets [a] ConfigMaps [b] not found");
        testActor.create(secret(A));
        assertStatusResolvedRefs(filterOne, Condition.Status.FALSE, "Referenced ConfigMaps [b] not found");
        testActor.create(cm(B));
        assertStatusResolvedRefs(filterOne, Condition.Status.TRUE, null);
        return filterOne;
    }

    @Test
    void shouldEventuallyResolveWhenASecretCreatedFirst() {
        testActor.create(secret(A));
        KafkaProtocolFilter filterOne = testActor.create(filter(FILTER_ONE,
                "${secret:" + A + ":foo}", "${secret:" + B + ":foo}"));
        assertStatusResolvedRefs(filterOne, Condition.Status.FALSE, "Referenced Secrets [b] not found");
        testActor.create(secret(B));
        assertStatusResolvedRefs(filterOne, Condition.Status.TRUE, null);
    }

    @Test
    void shouldEventuallyResolveWhenAllSecretsCreatedFirst() {
        testActor.create(secret(A));
        testActor.create(secret(B));
        testActor.create(secret(C));
        KafkaProtocolFilter filterOne = testActor.create(filter(FILTER_ONE,
                "${secret:" + A + ":foo}",
                "${secret:" + B + ":foo}"));
        assertStatusResolvedRefs(filterOne, Condition.Status.TRUE, null);
    }

    @Test
    void shouldEventuallyResolveWhenSecretsAndConfigMapsFirst() {
        testActor.create(secret(A));
        testActor.create(cm(B));
        testActor.create(secret(C));
        KafkaProtocolFilter filterOne = testActor.create(filter(FILTER_ONE,
                "${secret:" + A + ":foo}",
                "${configmap:" + B + ":foo}"));
        assertStatusResolvedRefs(filterOne, Condition.Status.TRUE, null);
    }

    @Test
    void shouldUpdateStatusOnFilterModify() {
        shouldEventuallyResolveWhenFilterCreatedFirst();

        KafkaProtocolFilter filterOne = testActor.replace(filter(FILTER_ONE,
                "${secret:" + C + ":foo}", "${configmap:" + B + ":foo}"));
        assertStatusResolvedRefs(filterOne, Condition.Status.FALSE, "Referenced Secrets [c] not found");

        testActor.create(secret(C));
        assertStatusResolvedRefs(filterOne, Condition.Status.TRUE, null);
    }

    @Test
    void shouldUpdateStatusOnSecretModify() {
        var filterOne = createFilterFirst();

        testActor.resources(Secret.class).withName(A).edit(secret -> secret.edit()
                .addToData("baz", Base64.getEncoder().encodeToString("".getBytes(StandardCharsets.UTF_8)))
                .build());
        assertStatusResolvedRefs(filterOne, Condition.Status.TRUE, null);
    }

    @Test
    void shouldUpdateStatusOnSecretDelete() {
        var filterOne = createFilterFirst();

        testActor.delete(secret(A));
        assertStatusResolvedRefs(filterOne, Condition.Status.FALSE, "Referenced Secrets [a] not found");
    }

    @Test
    void shouldUpdateStatusOnConfigMapDelete() {
        var filterOne = createFilterFirst();

        testActor.delete(cm(B));
        assertStatusResolvedRefs(filterOne, Condition.Status.FALSE, "Referenced ConfigMaps [b] not found");
    }

    private KafkaProtocolFilter filter(String filterName, String refA, String refB) {
        // @formatter:off
        return new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                    .withName(filterName)
                .endMetadata()
                .withNewSpec()
                    .withType("org.example.Filter")
                    .withConfigTemplate(Map.of(
                            "normalProp", "normalValue",
                            "securePropA", refA,
                            "securePropB", refB))
                .endSpec()
                .build();
        // @formatter:on
    }

    private void assertStatusResolvedRefs(KafkaProtocolFilter cr,
                                          Condition.Status conditionStatus,
                                          String message) {
        AWAIT.alias("FilterStatusResolvedRefs").untilAsserted(() -> {
            var kpf = testActor.resources(KafkaProtocolFilter.class)
                    .withName(ResourcesUtil.name(cr)).get();
            assertThat(kpf.getStatus()).isNotNull();
            KafkaProtocolFilterStatusAssert
                    .assertThat(kpf.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(kpf)
                    .singleCondition()
                    .hasType(Condition.Type.ResolvedRefs)
                    .hasStatus(conditionStatus)
                    .hasMessage(message)
                    .hasObservedGenerationInSyncWithMetadataOf(kpf);
        });
    }

    Secret secret(String name) {
        // @formatter:off
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .addToData("foo", Base64.getEncoder().encodeToString("bar".getBytes(StandardCharsets.UTF_8)))
                .build();
        // @formatter:on
    }

    ConfigMap cm(String name) {
        // @formatter:off
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .addToData("foo", "bar")
                .build();
        // @formatter:on
    }

}

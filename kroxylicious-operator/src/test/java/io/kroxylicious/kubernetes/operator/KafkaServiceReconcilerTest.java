/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.operator.assertj.ConditionListAssert;
import io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(MockitoExtension.class)
class KafkaServiceReconcilerTest {

    public static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));

    public static final long OBSERVED_GENERATION = 1345L;

    // @formatter:off
    public static final KafkaService SERVICE = new KafkaServiceBuilder()
            .withNewMetadata()
                .withName("test-service")
                .withGeneration(OBSERVED_GENERATION)
            .endMetadata()
            .withNewSpec()
                .withNewTls()
                    .withNewCertificateRef()
                        .withName("my-secret")
                    .endCertificateRef()
                    .withNewTrustAnchorRef()
                        .withName("my-configmap")
                    .endTrustAnchorRef()
                .endTls()
            .endSpec()
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

    public static final Secret UNSUPPORTED_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withName("my-secret")
                .withUid("uid")
                .withResourceVersion("7742")
            .endMetadata()
            .addToData("key", "value")
            .build();

    public static final ConfigMap PEM_CONFIG_MAP = new ConfigMapBuilder()
            .withNewMetadata()
                .withName("my-configmap")
                .withUid("uid")
                .withResourceVersion("7782")
            .endMetadata()
            .addToData("ca-bundle.pem", "value")
            .build();

    public static final ConfigMap P12_CONFIG_MAP = new ConfigMapBuilder()
            .withNewMetadata()
                .withName("my-configmap")
                .withUid("uid")
                .withResourceVersion("3342")
            .endMetadata()
            .addToData("ca-bundle.p12", "value")
            .build();

    public static final ConfigMap JKS_CONFIG_MAP = new ConfigMapBuilder()
            .withNewMetadata()
                .withName("my-configmap")
                .withUid("uid")
                .withResourceVersion("1266")
            .endMetadata()
            .addToData("ca-bundle.jks", "value")
            .build();

    public static final ConfigMap UNSUPPORTED_CONFIG_MAP = new ConfigMapBuilder()
            .withNewMetadata()
                .withName("my-configmap")
                .withUid("uid")
                .withResourceVersion("8982")
            .endMetadata()
            .addToData("unsuppor.ted", "value")
            .build();

    // @formatter:on

    private KafkaServiceReconciler kafkaServiceReconciler;

    @BeforeEach
    void setUp() {
        kafkaServiceReconciler = new KafkaServiceReconciler(Clock.systemUTC());
    }

    @Test
    void shouldSetResolvedRefsToUnknown() {
        // given
        final KafkaService kafkaService = new KafkaServiceBuilder().withNewMetadata().withGeneration(OBSERVED_GENERATION).endMetadata().build();

        var reconciler = new KafkaServiceReconciler(TEST_CLOCK);

        Context<KafkaService> context = mock(Context.class);

        // when
        var update = reconciler.updateErrorStatus(kafkaService, context, new RuntimeException("Boom!"));

        // then
        assertThat(update).isNotNull();
        assertThat(update.getResource()).isPresent();
        OperatorAssertions.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(kafkaService)
                .singleCondition()
                .hasObservedGenerationInSyncWithMetadataOf(kafkaService)
                .isResolvedRefsUnknown("java.lang.RuntimeException", "Boom!")
                .hasLastTransitionTime(TEST_CLOCK.instant());

    }

    static List<Arguments> shouldSetResolvedRefs() {
        List<Arguments> result = new ArrayList<>();
        // no client cert, no trust
        {
            Context<KafkaService> context = mock(Context.class);
            mockGetSecret(context, Optional.empty());
            mockGetConfigMap(context, Optional.empty());
            result.add(Arguments.argumentSet("no tls",
                    new KafkaServiceBuilder(SERVICE).editSpec().withTls(null).endSpec().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        ////////////
        // trust bundle cases....

        // no client cert, dangling trust bundle
        {
            Context<KafkaService> context = mock(Context.class);
            mockGetSecret(context, Optional.empty());
            mockGetConfigMap(context, Optional.empty());
            result.add(Arguments.argumentSet("dangling trust bundle",
                    new KafkaServiceBuilder(SERVICE).editSpec().editTls().withCertificateRef(null).endTls().endSpec().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.tls.trustAnchorRef: referenced resource not found")));
        }

        // no client cert, unsupported trust bundle kind
        {
            Context<KafkaService> context = mock(Context.class);
            mockGetSecret(context, Optional.empty());
            mockGetConfigMap(context, Optional.of(UNSUPPORTED_CONFIG_MAP));
            result.add(Arguments.argumentSet("unsupported trustAnchorRef kind",
                    new KafkaServiceBuilder(SERVICE).editSpec().editTls()
                            .withCertificateRef(null)
                            .editTrustAnchorRef().withKind("Unsupported").endTrustAnchorRef()
                            .endTls().endSpec().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REF_GROUP_KIND_NOT_SUPPORTED,
                                    "spec.tls.trustAnchorRef supports referents: configmaps")));
        }

        // no client cert, trust bundle ref missing key
        {
            Context<KafkaService> context = mock(Context.class);
            mockGetSecret(context, Optional.empty());
            mockGetConfigMap(context, Optional.of(UNSUPPORTED_CONFIG_MAP));
            result.add(Arguments.argumentSet("trust bundle ref missing key",
                    new KafkaServiceBuilder(SERVICE).editSpec().editTls()
                            .withCertificateRef(null)
                            .endTls().endSpec().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_INVALID,
                                    "spec.tls.trustAnchorRef must specify 'key'")));
        }

        // no client cert, unsupported trust bundle content
        {
            Context<KafkaService> context = mock(Context.class);
            mockGetSecret(context, Optional.empty());
            mockGetConfigMap(context, Optional.of(UNSUPPORTED_CONFIG_MAP));
            result.add(Arguments.argumentSet("unsupported trust bundle contents",
                    new KafkaServiceBuilder(SERVICE).editSpec().editTls()
                            .withCertificateRef(null)
                            .editTrustAnchorRef().withKey("ca-bundle.bob").endTrustAnchorRef()
                            .endTls().endSpec().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_INVALID,
                                    "spec.tls.trustAnchorRef.key should end with .pem, .p12 or .jks")));
        }

        // no client cert, pem trust bundle
        {
            Context<KafkaService> context = mock(Context.class);
            mockGetSecret(context, Optional.empty());
            mockGetConfigMap(context, Optional.of(PEM_CONFIG_MAP));
            result.add(Arguments.argumentSet("pem trust bundle",
                    new KafkaServiceBuilder(SERVICE).editSpec().editTls()
                            .withCertificateRef(null)
                            .editTrustAnchorRef().withKey("ca-bundle.pem").endTrustAnchorRef()
                            .endTls().endSpec().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }
        // no client cert, p12 trust bundle
        {
            Context<KafkaService> context = mock(Context.class);
            mockGetSecret(context, Optional.empty());
            mockGetConfigMap(context, Optional.of(P12_CONFIG_MAP));
            result.add(Arguments.argumentSet("p12 trust bundle",
                    new KafkaServiceBuilder(SERVICE).editSpec().editTls()
                            .withCertificateRef(null)
                            .editTrustAnchorRef().withKey("ca-bundle.p12").endTrustAnchorRef()
                            .endTls().endSpec().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }
        // no client cert, jks trust bundle
        {
            Context<KafkaService> context = mock(Context.class);
            mockGetSecret(context, Optional.empty());
            mockGetConfigMap(context, Optional.of(JKS_CONFIG_MAP));
            result.add(Arguments.argumentSet("jks trust bundle",
                    new KafkaServiceBuilder(SERVICE).editSpec().editTls()
                            .withCertificateRef(null)
                            .editTrustAnchorRef().withKey("ca-bundle.jks").endTrustAnchorRef()
                            .endTls().endSpec().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        ////////////
        // client certificate cases....

        // dangling client cert, no trust
        {
            Context<KafkaService> context = mock(Context.class);
            mockGetSecret(context, Optional.empty());
            mockGetConfigMap(context, Optional.empty());
            result.add(Arguments.argumentSet("dangling client cert",
                    new KafkaServiceBuilder(SERVICE).editSpec().editTls().withTrustAnchorRef(null).endTls().endSpec().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.tls.certificateRef: referenced resource not found")));
        }

        // unsupported client cert kind, no trust
        {
            Context<KafkaService> context = mock(Context.class);
            mockGetSecret(context, Optional.empty());
            mockGetConfigMap(context, Optional.empty());
            result.add(Arguments.argumentSet("unsupported client cert kind",
                    new KafkaServiceBuilder(SERVICE).editSpec().editTls()
                            .withTrustAnchorRef(null)
                            .editCertificateRef().withKind("Unsupported").endCertificateRef()
                            .endTls().endSpec().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REF_GROUP_KIND_NOT_SUPPORTED,
                                    "spec.tls.certificateRef: supports referents: secrets")));
        }

        // unsupported client cert in Secret, no trust
        {
            Context<KafkaService> context = mock(Context.class);
            mockGetSecret(context, Optional.of(UNSUPPORTED_SECRET));
            mockGetConfigMap(context, Optional.empty());
            result.add(Arguments.argumentSet("unsupported client cert Secret content",
                    new KafkaServiceBuilder(SERVICE).editSpec().editTls()
                            .withTrustAnchorRef(null)
                            .endTls().endSpec().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_INVALID_REFERENCED_RESOURCE,
                                    "spec.tls.certificateRef: referenced secret should have 'type: kubernetes.io/tls'")));
        }

        // tls client cert, no trust
        {
            Context<KafkaService> context = mock(Context.class);
            mockGetSecret(context, Optional.of(TLS_SECRET));
            mockGetConfigMap(context, Optional.empty());
            result.add(Arguments.argumentSet("tls client cert",
                    new KafkaServiceBuilder(SERVICE).editSpec().editTls().withTrustAnchorRef(null).endTls().endSpec().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        return result;
    }

    private static void mockGetConfigMap(
                                         Context<KafkaService> noClientCertAndNoTrust,
                                         Optional<ConfigMap> empty) {
        when(noClientCertAndNoTrust.getSecondaryResource(ConfigMap.class, KafkaServiceReconciler.CONFIG_MAPS_EVENT_SOURCE_NAME)).thenReturn(empty);
    }

    private static void mockGetSecret(
                                      Context<KafkaService> noClientCertAndNoTrust,
                                      Optional<Secret> optional) {
        when(noClientCertAndNoTrust.getSecondaryResource(Secret.class, KafkaServiceReconciler.SECRETS_EVENT_SOURCE_NAME)).thenReturn(optional);
    }

    @ParameterizedTest
    @MethodSource
    void shouldSetResolvedRefs(KafkaService kafkaService, Context<KafkaService> context, Consumer<ConditionListAssert> asserter) {

        // When
        final UpdateControl<KafkaService> updateControl = kafkaServiceReconciler.reconcile(kafkaService, context);

        // Then
        assertThat(updateControl).isNotNull();
        assertThat(updateControl.getResource()).isPresent();
        var c = OperatorAssertions.assertThat(updateControl.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(kafkaService)
                .conditionList();
        asserter.accept(c);
    }

    @Test
    void shouldSetReferentAnnotationWhenCertificateRefSecretPresent() {
        Context<KafkaService> context = mock(Context.class);
        mockGetSecret(context, Optional.of(TLS_SECRET));
        KafkaService service = new KafkaServiceBuilder(SERVICE).editSpec().editTls().withTrustAnchorRef(null).endTls().endSpec().build();
        // When
        final UpdateControl<KafkaService> updateControl = kafkaServiceReconciler.reconcile(service, context);

        // Then
        assertThat(updateControl).isNotNull();
        assertThat(updateControl.isPatchResourceAndStatus()).isTrue();
        assertThat(updateControl.getResource()).isPresent().get().satisfies(kafkaService -> {
            assertThat(kafkaService.getMetadata().getAnnotations()).containsKey(MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION);
        });
    }

    @Test
    void shouldSetReferentAnnotationWhenTrustAnchorRefConfigMapPresent() {
        Context<KafkaService> context = mock(Context.class);
        mockGetConfigMap(context, Optional.of(PEM_CONFIG_MAP));
        KafkaService service = new KafkaServiceBuilder(SERVICE).editSpec().editTls().withCertificateRef(null).editTrustAnchorRef().withKey("ca-bundle.pem")
                .endTrustAnchorRef().endTls().endSpec().build();
        // When
        final UpdateControl<KafkaService> updateControl = kafkaServiceReconciler.reconcile(service, context);

        // Then
        assertThat(updateControl).isNotNull();
        assertThat(updateControl.isPatchResourceAndStatus()).isTrue();
        assertThat(updateControl.getResource()).isPresent().get().satisfies(kafkaService -> {
            assertThat(kafkaService.getMetadata().getAnnotations()).containsKey(MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION);
        });
    }

    @Test
    void shouldNotSetReferentAnnotationWhenServiceHasNoReferents() {
        Context<KafkaService> context = mock(Context.class);
        KafkaService service = new KafkaServiceBuilder(SERVICE).editSpec().editTls().withTrustAnchorRef(null).endTls().endSpec().build();
        // When
        final UpdateControl<KafkaService> updateControl = kafkaServiceReconciler.reconcile(service, context);

        // Then
        assertThat(updateControl).isNotNull();
        assertThat(updateControl.isPatchResourceAndStatus()).isTrue();
        assertThat(updateControl.getResource()).isPresent().get().satisfies(kafkaService -> {
            assertThat(kafkaService.getMetadata().getAnnotations()).doesNotContainKey(MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION);
        });
    }

    @Test
    void canMapFromKafkaServiceWithTrustAnchorToConfigMap() {
        // Given
        var mapper = KafkaServiceReconciler.kafkaServiceToConfigMap();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(SERVICE);

        // Then
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(PEM_CONFIG_MAP));
    }

    @Test
    void canMapFromKafkaServiceWithoutTrustAnchorToConfigMap() {
        // Given
        var mapper = KafkaServiceReconciler.kafkaServiceToConfigMap();
        var serviceNoTrustAnchor = new KafkaServiceBuilder(SERVICE).editSpec().editTls().withTrustAnchorRef(null).endTls().endSpec().build();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(serviceNoTrustAnchor);

        // Then
        assertThat(secondaryResourceIDs).isEmpty();
    }

    @Test
    void canMapFromConfigMapToKafkaService() {
        // Given
        EventSourceContext<KafkaService> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KubernetesResourceList<KafkaService> mockList = mockKafkaServiceListOperation(client);
        when(mockList.getItems()).thenReturn(List.of(SERVICE));

        var mapper = KafkaServiceReconciler.configMapToKafkaService(eventSourceContext);

        // When
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(PEM_CONFIG_MAP);

        // Then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(SERVICE));
    }

    static Stream<Arguments> mappingToConfigMapToleratesKafkaServicesWithoutTls() {
        return Stream.of(
                Arguments.argumentSet("without tls", new KafkaServiceBuilder(SERVICE).editSpec().withTls(null).endSpec().build()),
                Arguments.argumentSet("with tls but without trust anchor",
                        new KafkaServiceBuilder(SERVICE).editSpec().editTls().withTrustAnchorRef(null).endTls().endSpec().build()));
    }

    @ParameterizedTest
    @MethodSource
    void mappingToConfigMapToleratesKafkaServicesWithoutTls(KafkaService service) {
        // Given
        EventSourceContext<KafkaService> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KubernetesResourceList<KafkaService> mockList = mockKafkaServiceListOperation(client);
        when(mockList.getItems()).thenReturn(List.of(service));

        // When
        var mapper = KafkaServiceReconciler.configMapToKafkaService(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(new ConfigMapBuilder().withNewMetadata().withName("cm").endMetadata().build());
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void canMapFromKafkaServiceWithClientCertToSecret() {
        // Given
        var mapper = KafkaServiceReconciler.kafkaServiceToSecret();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(SERVICE);

        // Then
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(TLS_SECRET));
    }

    @Test
    void canMapFromKafkaServiceWithoutClientCertToSecret() {
        // Given
        var mapper = KafkaServiceReconciler.kafkaServiceToSecret();
        var serviceNoCert = new KafkaServiceBuilder(SERVICE).editSpec().editTls().withCertificateRef(null).endTls().endSpec().build();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(serviceNoCert);

        // Then
        assertThat(secondaryResourceIDs).isEmpty();
    }

    static Stream<Arguments> mappingToSecretToleratesKafkaServicesWithoutTls() {
        return Stream.of(
                Arguments.argumentSet("without tls", new KafkaServiceBuilder(SERVICE).editSpec().withTls(null).endSpec().build()),
                Arguments.argumentSet("with tls but without client cert",
                        new KafkaServiceBuilder(SERVICE).editSpec().editTls().withCertificateRef(null).endTls().endSpec().build()));
    }

    @ParameterizedTest
    @MethodSource
    void mappingToSecretToleratesKafkaServicesWithoutTls(KafkaService service) {
        // Given
        EventSourceContext<KafkaService> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KubernetesResourceList<KafkaService> mockList = mockKafkaServiceListOperation(client);
        when(mockList.getItems()).thenReturn(List.of(service));

        // When
        var mapper = KafkaServiceReconciler.secretToKafkaService(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(new SecretBuilder().withNewMetadata().withName("secret").endMetadata().build());
        assertThat(primaryResourceIDs).isEmpty();
    }

    private KubernetesResourceList<KafkaService> mockKafkaServiceListOperation(KubernetesClient client) {
        MixedOperation<KafkaService, KubernetesResourceList<KafkaService>, Resource<KafkaService>> mockOperation = mock();
        when(client.resources(KafkaService.class)).thenReturn(mockOperation);
        KubernetesResourceList<KafkaService> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        return mockList;
    }

}

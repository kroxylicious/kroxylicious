/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.common.IngressRefBuilder;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.common.ProxyRefBuilder;
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

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.findOnlyResourceNamed;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toByNameMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ResourcesUtilTest {

    public static final String RESOURCE_NAME = "name";

    @Test
    void rfc1035DnsLabel() {
        assertThat(ResourcesUtil.isDnsLabel("", true)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("a", true)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("ab", true)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("a1", true)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("a-b", true)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("a-1", true)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("1", true)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("1a", true)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("1-a", true)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("-", true)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("-a", true)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("a-", true)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("-1", true)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("a".repeat(63), true)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("a".repeat(64), true)).isFalse();
    }

    @Test
    void rfc1123DnsLabel() {
        assertThat(ResourcesUtil.isDnsLabel("", false)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("a", false)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("ab", false)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("a1", false)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("a-b", false)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("a-1", false)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("1", false)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("1a", false)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("1-a", false)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("-", false)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("-a", false)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("a-", false)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("-1", false)).isFalse();
        assertThat(ResourcesUtil.isDnsLabel("a".repeat(63), false)).isTrue();
        assertThat(ResourcesUtil.isDnsLabel("a".repeat(64), false)).isFalse();
    }

    @Test
    void findOnlyResourceNamedWithEmptyCollection() {
        Optional<HasMetadata> resource = findOnlyResourceNamed(RESOURCE_NAME, Set.of());
        assertThat(resource).isEmpty();
    }

    @Test
    void findOnlyResourceNamedWithNoMatches() {
        Secret other = new SecretBuilder().withNewMetadata().withName("other").endMetadata().build();
        Optional<HasMetadata> resource = findOnlyResourceNamed(RESOURCE_NAME, Set.of(other));
        assertThat(resource).isEmpty();
    }

    @Test
    void findOnlyResourceNamedWithMatch() {
        Secret other = new SecretBuilder().withNewMetadata().withName(RESOURCE_NAME).endMetadata().build();
        Optional<HasMetadata> resource = findOnlyResourceNamed(RESOURCE_NAME, Set.of(other));
        assertThat(resource).isNotEmpty().contains(other);
    }

    @Test
    void findOnlyResourceNamedWithMultipleMatch() {
        Secret other = new SecretBuilder().withNewMetadata().withName(RESOURCE_NAME).endMetadata().build();
        List<HasMetadata> withMultipleSameName = List.of(other, other);
        assertThatThrownBy(() -> {
            findOnlyResourceNamed(RESOURCE_NAME, withMultipleSameName);
        }).isInstanceOf(IllegalStateException.class).hasMessage("collection contained more than one resource named " + RESOURCE_NAME);
    }

    @Test
    void filtersResourceIdsInSameNamespace() {
        // Given
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withNamespace("ns").withName("foo").endMetadata().build();
        EventSourceContext<?> eventSourceContext = prepareMockContextToProduceList(List.of(cm), ConfigMap.class);
        HasMetadata primary = new SecretBuilder().withNewMetadata().withNamespace("ns").withName("primary").endMetadata().build();

        // When
        var resources = ResourcesUtil.filteredResourceIdsInSameNamespace(eventSourceContext, primary, ConfigMap.class, p -> true);

        // Then
        assertThat(resources).containsExactly(ResourceID.fromResource(cm));
    }

    @Test
    void findReferrers() {
        // Given
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withNamespace("ns").withName("foo").addToAnnotations("ref", "primary").endMetadata().build();
        EventSourceContext<?> eventSourceContext = prepareMockContextToProduceList(List.of(cm), ConfigMap.class);
        HasMetadata primary = new SecretBuilder().withNewMetadata().withNamespace("ns").withName("primary").endMetadata().build();

        // When
        var resources = ResourcesUtil.findReferrers(eventSourceContext, primary, ConfigMap.class,
                configMap -> Optional.of(new AnyLocalRefBuilder().withName(configMap.getMetadata().getAnnotations().get("ref")).build()));

        // Then
        assertThat(resources).containsExactly(ResourceID.fromResource(cm));
    }

    @Test
    void findReferrersMultiShouldMapEmptyRefsToEmptySet() {
        // Given
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withNamespace("ns").withName("foo").addToAnnotations("ref", "primary").endMetadata().build();
        EventSourceContext<?> eventSourceContext = prepareMockContextToProduceList(List.of(cm), ConfigMap.class);
        HasMetadata primary = new SecretBuilder().withNewMetadata().withNamespace("ns").withName("primary").endMetadata().build();

        // When
        var resources = ResourcesUtil.findReferrersMulti(eventSourceContext, primary, ConfigMap.class,
                configMap -> Set.of());

        // Then
        assertThat(resources).isEmpty();
    }

    @Test
    void findReferrersMultiShouldMapSingleRefToResourceInContext() {
        // Given
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withNamespace("ns").withName("foo").addToAnnotations("ref", "primary").endMetadata().build();
        EventSourceContext<?> eventSourceContext = prepareMockContextToProduceList(List.of(cm), ConfigMap.class);
        HasMetadata primary = new SecretBuilder().withNewMetadata().withNamespace("ns").withName("primary").endMetadata().build();

        // When
        var resources = ResourcesUtil.findReferrersMulti(eventSourceContext, primary, ConfigMap.class,
                configMap -> Set.of(new AnyLocalRefBuilder().withName(configMap.getMetadata().getAnnotations().get("ref")).build()));

        // Then
        assertThat(resources).containsExactly(ResourceID.fromResource(cm));
    }

    @Test
    void findReferrersMultiShouldMapMultipleRefsToCorrespondingResourcesInContext() {
        // Given
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withNamespace("ns").withName("foo").addToAnnotations("ref", "primary").endMetadata().build();
        ConfigMap cm2 = new ConfigMapBuilder().withNewMetadata().withNamespace("ns").withName("bar").addToAnnotations("ref", "primary").endMetadata().build();
        EventSourceContext<?> eventSourceContext = prepareMockContextToProduceList(List.of(cm, cm2), ConfigMap.class);
        HasMetadata primary = new SecretBuilder().withNewMetadata().withNamespace("ns").withName("primary").endMetadata().build();

        // When
        var resources = ResourcesUtil.findReferrersMulti(eventSourceContext, primary, ConfigMap.class,
                configMap -> Set.of(new AnyLocalRefBuilder().withName(configMap.getMetadata().getAnnotations().get("ref")).build()));

        // Then
        assertThat(resources).containsExactlyInAnyOrder(ResourceID.fromResource(cm), ResourceID.fromResource(cm2));
    }

    @Test
    void findReferrersMultiShouldExcludeResourcesThatDontReferencePrimary() {
        // Given
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withNamespace("ns").withName("foo").addToAnnotations("ref", "primary").endMetadata().build();
        ConfigMap cm2 = new ConfigMapBuilder().withNewMetadata().withNamespace("ns").withName("bar").addToAnnotations("ref", "not-primary").endMetadata().build();
        EventSourceContext<?> eventSourceContext = prepareMockContextToProduceList(List.of(cm, cm2), ConfigMap.class);
        HasMetadata primary = new SecretBuilder().withNewMetadata().withNamespace("ns").withName("primary").endMetadata().build();

        // When
        var resources = ResourcesUtil.findReferrersMulti(eventSourceContext, primary, ConfigMap.class,
                configMap -> Set.of(new AnyLocalRefBuilder().withName(configMap.getMetadata().getAnnotations().get("ref")).build()));

        // Then
        assertThat(resources).containsExactlyInAnyOrder(ResourceID.fromResource(cm));
    }

    @Test
    void findReferrersMultiShouldHandleAllSecondariesDontReferencePrimary() {
        // Given
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withNamespace("ns").withName("foo").addToAnnotations("ref", "not-primary").endMetadata().build();
        ConfigMap cm2 = new ConfigMapBuilder().withNewMetadata().withNamespace("ns").withName("bar").addToAnnotations("ref", "not-primary").endMetadata().build();
        EventSourceContext<?> eventSourceContext = prepareMockContextToProduceList(List.of(cm, cm2), ConfigMap.class);
        HasMetadata primary = new SecretBuilder().withNewMetadata().withNamespace("ns").withName("primary").endMetadata().build();

        // When
        var resources = ResourcesUtil.findReferrersMulti(eventSourceContext, primary, ConfigMap.class,
                configMap -> Set.of(new AnyLocalRefBuilder().withName(configMap.getMetadata().getAnnotations().get("ref")).build()));

        // Then
        assertThat(resources).isEmpty();
    }

    @Test
    void findReferrersMultiShouldTolerateNullReferencesCollection() {
        // Given
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withNamespace("ns").withName("foo").addToAnnotations("ref", "not-primary").endMetadata().build();
        EventSourceContext<?> eventSourceContext = prepareMockContextToProduceList(List.of(cm), ConfigMap.class);
        HasMetadata primary = new SecretBuilder().withNewMetadata().withNamespace("ns").withName("primary").endMetadata().build();

        // When
        var resources = ResourcesUtil.findReferrersMulti(eventSourceContext, primary, ConfigMap.class,
                configMap -> null);

        // Then
        assertThat(resources).isEmpty();
    }

    @Test
    void findReferrersSupportsResourcesWithoutReferences() {
        // Given
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withNamespace("ns").withName("foo").addToAnnotations("ref", "primary").endMetadata().build();
        EventSourceContext<?> eventSourceContext = prepareMockContextToProduceList(List.of(cm), ConfigMap.class);
        HasMetadata primary = new SecretBuilder().withNewMetadata().withNamespace("ns").withName("primary").endMetadata().build();

        // When
        var resources = ResourcesUtil.findReferrers(eventSourceContext, primary, ConfigMap.class, configMap -> Optional.empty());

        // Then
        assertThat(resources).isEmpty();
    }

    @Test
    void toByNameMapEmptyStream() {
        Map<String, HasMetadata> hasMetadataMap = Stream.<HasMetadata> empty().collect(toByNameMap());
        assertThat(hasMetadataMap).isNotNull().isEmpty();
    }

    @Test
    void toByNameMapWithElements() {
        Secret a = new SecretBuilder().withNewMetadata().withName("a").endMetadata().build();
        Secret b = new SecretBuilder().withNewMetadata().withName("b").endMetadata().build();
        Map<String, Secret> hasMetadataMap = Stream.of(a, b).collect(toByNameMap());
        assertThat(hasMetadataMap).isNotNull().containsEntry("a", a).containsEntry("b", b);
    }

    @Test
    void toByNameMapDoesNotTolerateDuplicateKeys() {
        Secret a = new SecretBuilder().withNewMetadata().withName("a").endMetadata().build();
        Secret b = new SecretBuilder().withNewMetadata().withName("b").endMetadata().build();
        Secret c = new SecretBuilder().withNewMetadata().withName("b").endMetadata().build();
        Stream<Secret> stream = Stream.of(a, b, c);
        Collector<Secret, ?, Map<String, Secret>> collector = toByNameMap();
        assertThatThrownBy(() -> stream.collect(collector))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate key b");
    }

    @Test
    void name() {
        Secret secret = new SecretBuilder().withNewMetadata().withName(RESOURCE_NAME).endMetadata().build();
        assertThat(ResourcesUtil.name(secret)).isEqualTo(RESOURCE_NAME);
    }

    @Test
    void namespace() {
        String namespace = "namespace";
        Secret secret = new SecretBuilder().withNewMetadata().withNamespace(namespace).endMetadata().build();
        assertThat(ResourcesUtil.namespace(secret)).isEqualTo(namespace);
    }

    @Test
    void uid() {
        String uid = "uid";
        Secret secret = new SecretBuilder().withNewMetadata().withUid(uid).endMetadata().build();
        assertThat(ResourcesUtil.uid(secret)).isEqualTo(uid);
    }

    @Test
    void generation() {
        long generation = 123L;
        Secret secret = new SecretBuilder().withNewMetadata().withGeneration(generation).endMetadata().build();
        assertThat(ResourcesUtil.generation(secret)).isEqualTo(generation);
    }

    @Test
    void toLocalRefNoGroup() {
        assertThat(ResourcesUtil.toLocalRef(new SecretBuilder().withNewMetadata().withName("name").endMetadata().build()))
                .isEqualTo(new AnyLocalRefBuilder().withName("name").withGroup("").withKind("Secret").build());
    }

    @Test
    void toLocalRefWithGroup() {
        assertThat(ResourcesUtil.toLocalRef(new KafkaProxyBuilder().withNewMetadata().withName("name").endMetadata().build()))
                .isEqualTo(new AnyLocalRefBuilder().withName("name").withGroup("kroxylicious.io").withKind("KafkaProxy").build());
    }

    @Test
    void slugWithNamespaceEmptyGroup() {
        Secret secret = new SecretBuilder().withNewMetadata().withNamespace("my-ns").withName("secreto").endMetadata().build();
        assertThat(ResourcesUtil.namespacedSlug(ResourcesUtil.toLocalRef(secret), secret)).isEqualTo("secret/secreto in namespace 'my-ns'");
    }

    @Test
    void slugWithNamespaceNonEmptyGroup() {
        KafkaProxy secret = new KafkaProxyBuilder().withNewMetadata().withNamespace("my-ns").withName("secreto").endMetadata().build();
        assertThat(ResourcesUtil.namespacedSlug(ResourcesUtil.toLocalRef(secret), secret)).isEqualTo("kafkaproxy.kroxylicious.io/secreto in namespace 'my-ns'");
    }

    @Test
    void toLocalRef() {
        assertThat(ResourcesUtil.toLocalRef(new KafkaProxyBuilder().withNewMetadata().withName("foo").endMetadata().build()))
                .isEqualTo(new ProxyRefBuilder().withName("foo").build());

        assertThat(ResourcesUtil.toLocalRef(new KafkaServiceBuilder().withNewMetadata().withName("foo").endMetadata().build()))
                .isEqualTo(new KafkaServiceRefBuilder().withName("foo").build());

        assertThat(ResourcesUtil.toLocalRef(new KafkaProxyIngressBuilder().withNewMetadata().withName("foo").endMetadata().build()))
                .isEqualTo(new IngressRefBuilder().withName("foo").build());

        assertThat(ResourcesUtil.toLocalRef(new GenericKubernetesResourceBuilder()
                .withKind("KafkaProtocolFilter")
                .withApiVersion("filter.kroxylicious.io/v1alpha1")
                .withNewMetadata().withName("foo").endMetadata().build()))
                .isEqualTo(new FilterRefBuilder().withName("foo").build());
    }

    public static Stream<Arguments> isStatusFresh_VirtualCluster() {
        VirtualKafkaCluster observedGenerationEqualsMetadataGeneration = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                .withGeneration(1L)
                .endMetadata()
                .editStatus().withObservedGeneration(1L)
                .endStatus().build();
        VirtualKafkaCluster observedGenerationLessThanMetadataGeneration = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata()
                .editStatus().withObservedGeneration(1L)
                .endStatus().build();
        VirtualKafkaCluster observedGenerationNull = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata()
                .editStatus().withObservedGeneration(null)
                .endStatus().build();
        VirtualKafkaCluster statusNull = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata().build();
        return Stream.of(Arguments.argumentSet("observed generation equals metadata generation", observedGenerationEqualsMetadataGeneration, true),
                Arguments.argumentSet("observed generation less than metadata generation", observedGenerationLessThanMetadataGeneration, false),
                Arguments.argumentSet("observed generation null", observedGenerationNull, false),
                Arguments.argumentSet("status null", statusNull, false));
    }

    @ParameterizedTest
    @MethodSource
    void isStatusFresh_VirtualCluster(VirtualKafkaCluster cluster, boolean isReconciled) {
        assertThat(ResourcesUtil.isStatusFresh(cluster)).isEqualTo(isReconciled);
    }

    public static Stream<Arguments> hasResolvedRefsFalse() {
        Condition resolvedRefsTrueCondition = resolvedRefsCondition(Condition.Status.TRUE);
        Condition resolvedRefsFalseCondition = resolvedRefsCondition(Condition.Status.FALSE);
        VirtualKafkaCluster vkcNoStatus = new VirtualKafkaClusterBuilder().build();
        VirtualKafkaCluster vkcNoConditions = new VirtualKafkaClusterBuilder().withNewStatus().endStatus().build();
        VirtualKafkaCluster vkcEmptyConditions = new VirtualKafkaClusterBuilder().withNewStatus().withConditions(List.of()).endStatus().build();
        VirtualKafkaCluster vkcResolvedRefsTrue = new VirtualKafkaClusterBuilder().withNewStatus().withConditions(
                resolvedRefsTrueCondition).endStatus().build();
        VirtualKafkaCluster vkcResolvedRefsFalse = new VirtualKafkaClusterBuilder().withNewStatus().withConditions(
                resolvedRefsFalseCondition).endStatus().build();

        KafkaService ksNoStatus = new KafkaServiceBuilder().build();
        KafkaService ksNoConditions = new KafkaServiceBuilder().withNewStatus().endStatus().build();
        KafkaService ksEmptyConditions = new KafkaServiceBuilder().withNewStatus().withConditions(List.of()).endStatus().build();
        KafkaService ksResolvedRefsTrue = new KafkaServiceBuilder().withNewStatus().withConditions(
                resolvedRefsTrueCondition).endStatus().build();
        KafkaService ksResolvedRefsFalse = new KafkaServiceBuilder().withNewStatus().withConditions(
                resolvedRefsFalseCondition).endStatus().build();

        KafkaProtocolFilter kpfNoStatus = new KafkaProtocolFilterBuilder().build();
        KafkaProtocolFilter kpfNoConditions = new KafkaProtocolFilterBuilder().withNewStatus().endStatus().build();
        KafkaProtocolFilter kpfEmptyConditions = new KafkaProtocolFilterBuilder().withNewStatus().withConditions(List.of()).endStatus().build();
        KafkaProtocolFilter kpfResolvedRefsTrue = new KafkaProtocolFilterBuilder().withNewStatus().withConditions(
                resolvedRefsTrueCondition).endStatus().build();
        KafkaProtocolFilter kpfResolvedRefsFalse = new KafkaProtocolFilterBuilder().withNewStatus().withConditions(
                resolvedRefsFalseCondition).endStatus().build();

        KafkaProxyIngress kpiNoStatus = new KafkaProxyIngressBuilder().build();
        KafkaProxyIngress kpiNoConditions = new KafkaProxyIngressBuilder().withNewStatus().endStatus().build();
        KafkaProxyIngress kpiEmptyConditions = new KafkaProxyIngressBuilder().withNewStatus().withConditions(List.of()).endStatus().build();
        KafkaProxyIngress kpiResolvedRefsTrue = new KafkaProxyIngressBuilder().withNewStatus().withConditions(
                resolvedRefsTrueCondition).endStatus().build();
        KafkaProxyIngress kpiResolvedRefsFalse = new KafkaProxyIngressBuilder().withNewStatus().withConditions(
                resolvedRefsFalseCondition).endStatus().build();

        return Stream.of(Arguments.argumentSet("virtualkafkacluster - no status", vkcNoStatus, false),
                Arguments.argumentSet("virtualkafkacluster - no conditions on status", vkcNoConditions, false),
                Arguments.argumentSet("virtualkafkacluster - empty conditions on status", vkcEmptyConditions, false),
                Arguments.argumentSet("virtualkafkacluster - resolved refs true", vkcResolvedRefsTrue, false),
                Arguments.argumentSet("virtualkafkacluster - resolved refs false", vkcResolvedRefsFalse, true),
                Arguments.argumentSet("kafkaservice - no status", ksNoStatus, false),
                Arguments.argumentSet("kafkaservice - no conditions on status", ksNoConditions, false),
                Arguments.argumentSet("kafkaservice - empty conditions on status", ksEmptyConditions, false),
                Arguments.argumentSet("kafkaservice - resolved refs true", ksResolvedRefsTrue, false),
                Arguments.argumentSet("kafkaservice - resolved refs false", ksResolvedRefsFalse, true),
                Arguments.argumentSet("kafkaprotocolfilter - no status", kpfNoStatus, false),
                Arguments.argumentSet("kafkaprotocolfilter - no conditions on status", kpfNoConditions, false),
                Arguments.argumentSet("kafkaprotocolfilter - empty conditions on status", kpfEmptyConditions, false),
                Arguments.argumentSet("kafkaprotocolfilter - resolved refs true", kpfResolvedRefsTrue, false),
                Arguments.argumentSet("kafkaprotocolfilter - resolved refs false", kpfResolvedRefsFalse, true),
                Arguments.argumentSet("kafkaproxyingress - no status", kpiNoStatus, false),
                Arguments.argumentSet("kafkaproxyingress - no conditions on status", kpiNoConditions, false),
                Arguments.argumentSet("kafkaproxyingress - empty conditions on status", kpiEmptyConditions, false),
                Arguments.argumentSet("kafkaproxyingress - resolved refs true", kpiResolvedRefsTrue, false),
                Arguments.argumentSet("kafkaproxyingress - resolved refs false", kpiResolvedRefsFalse, true));
    }

    @NonNull
    private static Condition resolvedRefsCondition(Condition.Status status) {
        return new ConditionBuilder()
                .withLastTransitionTime(Instant.EPOCH)
                .withMessage("message")
                .withObservedGeneration(1L)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(
                        status)
                .withReason("reason").build();
    }

    @ParameterizedTest
    @MethodSource
    void hasResolvedRefsFalse(HasMetadata cluster, boolean hasResolvedRefsFalse) {
        assertThat(ResourcesUtil.hasResolvedRefsFalseCondition(cluster)).isEqualTo(hasResolvedRefsFalse);
    }

    @Test
    void hasResolvedRefsFalseThrowsWhenResourceDoesntUseResolveRefs() {
        assertThatThrownBy(() -> ResourcesUtil.hasResolvedRefsFalseCondition(new KafkaProxy())).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Resource kind 'KafkaProxy' does not use ResolveRefs conditions");
    }

    public static Stream<Arguments> isStatusFresh_KafkaProxyIngress() {
        KafkaProxyIngress observedGenerationEqualsMetadataGeneration = new KafkaProxyIngressBuilder()
                .withNewMetadata()
                .withGeneration(1L)
                .endMetadata()
                .editStatus().withObservedGeneration(1L)
                .endStatus().build();
        KafkaProxyIngress observedGenerationLessThanMetadataGeneration = new KafkaProxyIngressBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata()
                .editStatus().withObservedGeneration(1L)
                .endStatus().build();
        KafkaProxyIngress observedGenerationNull = new KafkaProxyIngressBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata()
                .editStatus().withObservedGeneration(null)
                .endStatus().build();
        KafkaProxyIngress statusNull = new KafkaProxyIngressBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata().build();
        return Stream.of(Arguments.argumentSet("observed generation equals metadata generation", observedGenerationEqualsMetadataGeneration, true),
                Arguments.argumentSet("observed generation less than metadata generation", observedGenerationLessThanMetadataGeneration, false),
                Arguments.argumentSet("observed generation null", observedGenerationNull, false),
                Arguments.argumentSet("status null", statusNull, false));
    }

    @ParameterizedTest
    @MethodSource
    void isStatusFresh_KafkaProxyIngress(KafkaProxyIngress ingress, boolean isReconciled) {
        assertThat(ResourcesUtil.isStatusFresh(ingress)).isEqualTo(isReconciled);
    }

    public static Stream<Arguments> isStatusFresh_KafkaProtocolFilter() {
        KafkaProtocolFilter observedGenerationEqualsMetadataGeneration = new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                .withGeneration(1L)
                .endMetadata()
                .editStatus().withObservedGeneration(1L)
                .endStatus().build();
        KafkaProtocolFilter observedGenerationLessThanMetadataGeneration = new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata()
                .editStatus().withObservedGeneration(1L)
                .endStatus().build();
        KafkaProtocolFilter observedGenerationNull = new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata()
                .editStatus().withObservedGeneration(null)
                .endStatus().build();
        KafkaProtocolFilter statusNull = new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata().build();
        return Stream.of(Arguments.argumentSet("observed generation equals metadata generation", observedGenerationEqualsMetadataGeneration, true),
                Arguments.argumentSet("observed generation less than metadata generation", observedGenerationLessThanMetadataGeneration, false),
                Arguments.argumentSet("observed generation null", observedGenerationNull, false),
                Arguments.argumentSet("status null", statusNull, false));
    }

    @ParameterizedTest
    @MethodSource
    void isStatusFresh_KafkaProtocolFilter(KafkaProtocolFilter filter, boolean isReconciled) {
        assertThat(ResourcesUtil.isStatusFresh(filter)).isEqualTo(isReconciled);
    }

    public static Stream<Arguments> isStatusFresh_KafkaProxy() {
        KafkaProxy observedGenerationEqualsMetadataGeneration = new KafkaProxyBuilder()
                .withNewMetadata()
                .withGeneration(1L)
                .endMetadata()
                .editStatus().withObservedGeneration(1L)
                .endStatus().build();
        KafkaProxy observedGenerationLessThanMetadataGeneration = new KafkaProxyBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata()
                .editStatus().withObservedGeneration(1L)
                .endStatus().build();
        KafkaProxy observedGenerationNull = new KafkaProxyBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata()
                .editStatus().withObservedGeneration(null)
                .endStatus().build();
        KafkaProxy statusNull = new KafkaProxyBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata().build();
        return Stream.of(Arguments.argumentSet("observed generation equals metadata generation", observedGenerationEqualsMetadataGeneration, true),
                Arguments.argumentSet("observed generation less than metadata generation", observedGenerationLessThanMetadataGeneration, false),
                Arguments.argumentSet("observed generation null", observedGenerationNull, false),
                Arguments.argumentSet("status null", statusNull, false));
    }

    @ParameterizedTest
    @MethodSource
    void isStatusFresh_KafkaProxy(KafkaProxy proxy, boolean isReconciled) {
        assertThat(ResourcesUtil.isStatusFresh(proxy)).isEqualTo(isReconciled);
    }

    public static Stream<Arguments> isStatusFresh_KafkaService() {
        KafkaService observedGenerationEqualsMetadataGeneration = new KafkaServiceBuilder()
                .withNewMetadata()
                .withGeneration(1L)
                .endMetadata()
                .editStatus().withObservedGeneration(1L)
                .endStatus().build();
        KafkaService observedGenerationLessThanMetadataGeneration = new KafkaServiceBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata()
                .editStatus().withObservedGeneration(1L)
                .endStatus().build();
        KafkaService observedGenerationNull = new KafkaServiceBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata()
                .editStatus().withObservedGeneration(null)
                .endStatus().build();
        KafkaService statusNull = new KafkaServiceBuilder()
                .withNewMetadata()
                .withGeneration(2L)
                .endMetadata().build();
        return Stream.of(Arguments.argumentSet("observed generation equals metadata generation", observedGenerationEqualsMetadataGeneration, true),
                Arguments.argumentSet("observed generation less than metadata generation", observedGenerationLessThanMetadataGeneration, false),
                Arguments.argumentSet("observed generation null", observedGenerationNull, false),
                Arguments.argumentSet("status null", statusNull, false));
    }

    @ParameterizedTest
    @MethodSource
    void isStatusFresh_KafkaService(KafkaService service, boolean isReconciled) {
        assertThat(ResourcesUtil.isStatusFresh(service)).isEqualTo(isReconciled);
    }

    private <T extends HasMetadata> EventSourceContext<?> prepareMockContextToProduceList(List<T> build, Class<T> clazz) {
        KubernetesResourceList<T> mock = mock();
        when(mock.getItems()).thenReturn(build);

        NonNamespaceOperation<T, KubernetesResourceList<T>, Resource<T>> nonNamespaceOperation = mock();
        when(nonNamespaceOperation.list()).thenReturn(mock);

        MixedOperation<T, KubernetesResourceList<T>, Resource<T>> resourceMixedOperation = mock();
        when(resourceMixedOperation.inNamespace(anyString())).thenReturn(nonNamespaceOperation);

        KubernetesClient client = mock();
        when(client.resources(clazz)).thenReturn(resourceMixedOperation);
        EventSourceContext<?> eventSourceContext = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        return eventSourceContext;
    }
}

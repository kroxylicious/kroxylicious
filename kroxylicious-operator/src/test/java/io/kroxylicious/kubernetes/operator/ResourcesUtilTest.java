/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Stream;

import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
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
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.common.IngressRefBuilder;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.common.ProxyRefBuilder;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Ingresses;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.operator.assertj.VirtualKafkaClusterStatusAssert;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.findOnlyResourceNamed;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toByNameMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ResourcesUtilTest {

    public static final String RESOURCE_NAME = "name";
    public static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
    protected static final ConfigMap EMPTY_CONFIG_NMAP = new ConfigMapBuilder().withData(Map.of()).build();

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
        return Stream.of(argumentSet("observed generation equals metadata generation", observedGenerationEqualsMetadataGeneration, true),
                argumentSet("observed generation less than metadata generation", observedGenerationLessThanMetadataGeneration, false),
                argumentSet("observed generation null", observedGenerationNull, false),
                argumentSet("status null", statusNull, false));
    }

    @ParameterizedTest
    @MethodSource
    void isStatusFresh_VirtualCluster(VirtualKafkaCluster cluster, boolean isReconciled) {
        assertThat(ResourcesUtil.isStatusFresh(cluster)).isEqualTo(isReconciled);
    }

    public static Stream<Arguments> hasResolvedRefsFalse() {
        long latestGeneration = 2L;
        long staleGeneration = 1L;
        Condition resolvedRefsTrueCondition = resolvedRefsCondition(Condition.Status.TRUE, latestGeneration);
        Condition resolvedRefsFalseCondition = resolvedRefsCondition(Condition.Status.FALSE, latestGeneration);
        Condition resolvedRefsFalseStaleCondition = resolvedRefsCondition(Condition.Status.FALSE, staleGeneration);
        var baseVkcBuilder = new VirtualKafkaClusterBuilder().withNewMetadata()
                .withGeneration(latestGeneration).endMetadata();
        VirtualKafkaCluster vkcNoStatus = baseVkcBuilder.build();
        VirtualKafkaCluster vkcNoConditions = new VirtualKafkaClusterBuilder(vkcNoStatus).withNewStatus().endStatus().build();
        VirtualKafkaCluster vkcEmptyConditions = new VirtualKafkaClusterBuilder(vkcNoStatus).withNewStatus().withConditions(List.of()).endStatus().build();
        VirtualKafkaCluster vkcResolvedRefsTrue = new VirtualKafkaClusterBuilder(vkcNoStatus).withNewStatus().withConditions(
                resolvedRefsTrueCondition).endStatus().build();
        VirtualKafkaCluster vkcResolvedRefsFalse = new VirtualKafkaClusterBuilder(vkcNoStatus).withNewStatus().withConditions(
                resolvedRefsFalseCondition).endStatus().build();
        VirtualKafkaCluster vkcResolvedRefsFalseStale = new VirtualKafkaClusterBuilder(vkcNoStatus).withNewStatus().withConditions(
                resolvedRefsFalseStaleCondition).endStatus().build();

        var baseKsBuilder = new KafkaServiceBuilder().withNewMetadata()
                .withGeneration(latestGeneration).endMetadata();
        KafkaService ksNoStatus = baseKsBuilder.build();
        KafkaService ksNoConditions = new KafkaServiceBuilder(ksNoStatus).withNewStatus().endStatus().build();
        KafkaService ksEmptyConditions = new KafkaServiceBuilder(ksNoStatus).withNewStatus().withConditions(List.of()).endStatus().build();
        KafkaService ksResolvedRefsTrue = new KafkaServiceBuilder(ksNoStatus).withNewStatus().withConditions(
                resolvedRefsTrueCondition).endStatus().build();
        KafkaService ksResolvedRefsFalse = new KafkaServiceBuilder(ksNoStatus).withNewStatus().withConditions(
                resolvedRefsFalseCondition).endStatus().build();
        KafkaService ksResolvedRefsFalseStale = new KafkaServiceBuilder(ksNoStatus).withNewStatus().withConditions(
                resolvedRefsFalseStaleCondition).endStatus().build();

        var baseKpfBuilder = new KafkaProtocolFilterBuilder().withNewMetadata()
                .withGeneration(latestGeneration).endMetadata();
        KafkaProtocolFilter kpfNoStatus = baseKpfBuilder.build();
        KafkaProtocolFilter kpfNoConditions = new KafkaProtocolFilterBuilder(kpfNoStatus).withNewStatus().endStatus().build();
        KafkaProtocolFilter kpfEmptyConditions = new KafkaProtocolFilterBuilder(kpfNoStatus).withNewStatus().withConditions(List.of()).endStatus().build();
        KafkaProtocolFilter kpfResolvedRefsTrue = new KafkaProtocolFilterBuilder(kpfNoStatus).withNewStatus().withConditions(
                resolvedRefsTrueCondition).endStatus().build();
        KafkaProtocolFilter kpfResolvedRefsFalse = new KafkaProtocolFilterBuilder(kpfNoStatus).withNewStatus().withConditions(
                resolvedRefsFalseCondition).endStatus().build();
        KafkaProtocolFilter kpfResolvedRefsFalseStale = new KafkaProtocolFilterBuilder(kpfNoStatus).withNewStatus().withConditions(
                resolvedRefsFalseStaleCondition).endStatus().build();

        var baseKpiBuilder = new KafkaProxyIngressBuilder().withNewMetadata()
                .withGeneration(latestGeneration).endMetadata();
        KafkaProxyIngress kpiNoStatus = baseKpiBuilder.build();
        KafkaProxyIngress kpiNoConditions = new KafkaProxyIngressBuilder(kpiNoStatus).withNewStatus().endStatus().build();
        KafkaProxyIngress kpiEmptyConditions = new KafkaProxyIngressBuilder(kpiNoStatus).withNewStatus().withConditions(List.of()).endStatus().build();
        KafkaProxyIngress kpiResolvedRefsTrue = new KafkaProxyIngressBuilder(kpiNoStatus).withNewStatus().withConditions(
                resolvedRefsTrueCondition).endStatus().build();
        KafkaProxyIngress kpiResolvedRefsFalse = new KafkaProxyIngressBuilder(kpiNoStatus).withNewStatus().withConditions(
                resolvedRefsFalseCondition).endStatus().build();
        KafkaProxyIngress kpiResolvedRefsFalseStale = new KafkaProxyIngressBuilder(kpiNoStatus).withNewStatus().withConditions(
                resolvedRefsFalseStaleCondition).endStatus().build();

        return Stream.of(argumentSet("virtualkafkacluster - no status", vkcNoStatus, false),
                argumentSet("virtualkafkacluster - no conditions on status", vkcNoConditions, false),
                argumentSet("virtualkafkacluster - empty conditions on status", vkcEmptyConditions, false),
                argumentSet("virtualkafkacluster - resolved refs true", vkcResolvedRefsTrue, false),
                argumentSet("virtualkafkacluster - resolved refs false", vkcResolvedRefsFalse, true),
                argumentSet("virtualkafkacluster - resolved refs false stale observedGeneration", vkcResolvedRefsFalseStale, false),
                argumentSet("kafkaservice - no status", ksNoStatus, false),
                argumentSet("kafkaservice - no conditions on status", ksNoConditions, false),
                argumentSet("kafkaservice - empty conditions on status", ksEmptyConditions, false),
                argumentSet("kafkaservice - resolved refs true", ksResolvedRefsTrue, false),
                argumentSet("kafkaservice - resolved refs false", ksResolvedRefsFalse, true),
                argumentSet("kafkaservice - resolved refs false stale observedGeneration", ksResolvedRefsFalseStale, false),
                argumentSet("kafkaprotocolfilter - no status", kpfNoStatus, false),
                argumentSet("kafkaprotocolfilter - no conditions on status", kpfNoConditions, false),
                argumentSet("kafkaprotocolfilter - empty conditions on status", kpfEmptyConditions, false),
                argumentSet("kafkaprotocolfilter - resolved refs true", kpfResolvedRefsTrue, false),
                argumentSet("kafkaprotocolfilter - resolved refs false", kpfResolvedRefsFalse, true),
                argumentSet("kafkaprotocolfilter - resolved refs false stale observedGeneration", kpfResolvedRefsFalseStale, false),
                argumentSet("kafkaproxyingress - no status", kpiNoStatus, false),
                argumentSet("kafkaproxyingress - no conditions on status", kpiNoConditions, false),
                argumentSet("kafkaproxyingress - empty conditions on status", kpiEmptyConditions, false),
                argumentSet("kafkaproxyingress - resolved refs true", kpiResolvedRefsTrue, false),
                argumentSet("kafkaproxyingress - resolved refs false", kpiResolvedRefsFalse, true),
                argumentSet("kafkaproxyingress - resolved refs false stale observedGeneration", kpiResolvedRefsFalseStale, false));
    }

    @NonNull
    private static Condition resolvedRefsCondition(Condition.Status status, long observedGeneration) {
        return new ConditionBuilder()
                .withLastTransitionTime(Instant.EPOCH)
                .withMessage("message")
                .withObservedGeneration(observedGeneration)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(
                        status)
                .withReason("reason").build();
    }

    @ParameterizedTest
    @MethodSource
    void hasResolvedRefsFalse(HasMetadata cluster, boolean hasResolvedRefsFalse) {
        assertThat(ResourcesUtil.hasFreshResolvedRefsFalseCondition(cluster)).isEqualTo(hasResolvedRefsFalse);
    }

    @Test
    void hasResolvedRefsFalseThrowsWhenResourceDoesntUseResolveRefs() {
        KafkaProxy kafkaProxy = new KafkaProxy();
        assertThatThrownBy(() -> ResourcesUtil.hasFreshResolvedRefsFalseCondition(kafkaProxy)).isInstanceOf(IllegalArgumentException.class)
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
        return Stream.of(argumentSet("observed generation equals metadata generation", observedGenerationEqualsMetadataGeneration, true),
                argumentSet("observed generation less than metadata generation", observedGenerationLessThanMetadataGeneration, false),
                argumentSet("observed generation null", observedGenerationNull, false),
                argumentSet("status null", statusNull, false));
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
        return Stream.of(argumentSet("observed generation equals metadata generation", observedGenerationEqualsMetadataGeneration, true),
                argumentSet("observed generation less than metadata generation", observedGenerationLessThanMetadataGeneration, false),
                argumentSet("observed generation null", observedGenerationNull, false),
                argumentSet("status null", statusNull, false));
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
        return Stream.of(argumentSet("observed generation equals metadata generation", observedGenerationEqualsMetadataGeneration, true),
                argumentSet("observed generation less than metadata generation", observedGenerationLessThanMetadataGeneration, false),
                argumentSet("observed generation null", observedGenerationNull, false),
                argumentSet("status null", statusNull, false));
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
        return Stream.of(argumentSet("observed generation equals metadata generation", observedGenerationEqualsMetadataGeneration, true),
                argumentSet("observed generation less than metadata generation", observedGenerationLessThanMetadataGeneration, false),
                argumentSet("observed generation null", observedGenerationNull, false),
                argumentSet("status null", statusNull, false));
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

    @ParameterizedTest
    @MethodSource("invalidTrustAnchorRefs")
    void shouldReturnResolvedRefsFalseStatusCondition(TrustAnchorRef trustAnchorRef,
                                                      @Nullable ConfigMap targetedConfigMap,
                                                      String expectedCondition,
                                                      ThrowingConsumer<String> stringThrowingConsumer) {
        // Given
        Ingresses ingress = new IngressesBuilder().withNewTls().withTrustAnchorRef(trustAnchorRef).endTls().build();
        VirtualKafkaCluster vkc = new VirtualKafkaClusterBuilder().withNewSpec().withIngresses(List.of(ingress)).endSpec().build();
        @SuppressWarnings("unchecked")
        Context<VirtualKafkaCluster> reconcilerContext = mock(Context.class);
        when(reconcilerContext.getSecondaryResource(ConfigMap.class, VirtualKafkaClusterReconciler.CONFIGMAPS_EVENT_SOURCE_NAME))
                .thenReturn(Optional.ofNullable(targetedConfigMap));

        // When
        ResourceCheckResult<VirtualKafkaCluster> actual = ResourcesUtil.checkTrustAnchorRef(vkc, reconcilerContext,
                VirtualKafkaClusterReconciler.CONFIGMAPS_EVENT_SOURCE_NAME, trustAnchorRef,
                "spec.ingresses[].tls.trustAnchor", new VirtualKafkaClusterStatusFactory(TEST_CLOCK));

        // Then
        assertThat(actual)
                .isNotNull()
                .satisfies(virtualKafkaClusterResourceCheckResult -> assertThat(virtualKafkaClusterResourceCheckResult.resource())
                        .isNotNull()
                        .satisfies(actualVkc -> VirtualKafkaClusterStatusAssert.assertThat(actualVkc.getStatus())
                                .singleCondition()
                                .isResolvedRefsFalse(expectedCondition, stringThrowingConsumer)));

    }

    @ParameterizedTest
    @CsvSource({ "key.pem", "key.p12", "key.jks" })
    void shouldAcceptSupportedKeyfileExtensions(String key) {
        // Given
        TrustAnchorRef trustAnchorRef = new TrustAnchorRefBuilder().withKey(key).withNewRef().withName("configmap").endRef().build();
        Ingresses ingress = new IngressesBuilder().withNewTls().withTrustAnchorRef(trustAnchorRef).endTls().build();
        VirtualKafkaCluster vkc = new VirtualKafkaClusterBuilder().withNewSpec().withIngresses(List.of(ingress)).endSpec().build();
        @SuppressWarnings("unchecked")
        Context<VirtualKafkaCluster> reconcilerContext = mock(Context.class);
        when(reconcilerContext.getSecondaryResource(ConfigMap.class, VirtualKafkaClusterReconciler.CONFIGMAPS_EVENT_SOURCE_NAME))
                .thenReturn(Optional.ofNullable(
                        new ConfigMapBuilder().withNewMetadata().withName("configmap").endMetadata().withData(Map.of(key, "I'm a key honnest")).build()));

        // When
        ResourceCheckResult<VirtualKafkaCluster> actual = ResourcesUtil.checkTrustAnchorRef(vkc, reconcilerContext,
                VirtualKafkaClusterReconciler.CONFIGMAPS_EVENT_SOURCE_NAME, trustAnchorRef,
                "spec.ingresses[].tls.trustAnchor", new VirtualKafkaClusterStatusFactory(TEST_CLOCK));

        // Then
        assertThat(actual).isNotNull()
                .satisfies(virtualKafkaClusterResourceCheckResult -> assertThat(virtualKafkaClusterResourceCheckResult.resource()).isNull());
    }

    static Stream<Arguments> invalidTrustAnchorRefs() {
        return Stream.of(
                argumentSet("Invalid reference to config map invalid kind.",
                        new TrustAnchorRefBuilder().withKey("key.pem").withNewRef().withKind("custom").withName("configmap").endRef().build(),
                        null,
                        Condition.REASON_REF_GROUP_KIND_NOT_SUPPORTED,
                        (ThrowingConsumer<String>) message -> assertThat(message).endsWith("supports referents: configmaps")),
                argumentSet("Invalid reference to config map invalid group.",
                        new TrustAnchorRefBuilder().withKey("key.pem").withNewRef().withGroup("custom").withName("configmap").endRef().build(),
                        null,
                        Condition.REASON_REF_GROUP_KIND_NOT_SUPPORTED,
                        (ThrowingConsumer<String>) message -> assertThat(message).endsWith("supports referents: configmaps")),
                argumentSet("Empty target config map.",
                        new TrustAnchorRefBuilder().withKey("key.pem").withNewRef().withName("configmap").endRef().build(),
                        null,
                        Condition.REASON_REFS_NOT_FOUND,
                        (ThrowingConsumer<String>) message -> assertThat(message).endsWith("referenced resource not found")),
                argumentSet("Empty target config map.",
                        new TrustAnchorRefBuilder().withKey("key.pem").withNewRef().withName("configmap").endRef().build(),
                        null,
                        Condition.REASON_REFS_NOT_FOUND,
                        (ThrowingConsumer<String>) message -> assertThat(message).endsWith("referenced resource not found")),
                argumentSet("ConfigMap not found",
                        new TrustAnchorRefBuilder().withNewRef().withName("unknown_config_map").endRef().build(),
                        null,
                        Condition.REASON_REFS_NOT_FOUND,
                        (ThrowingConsumer<String>) message -> assertThat(message).endsWith("referenced resource not found")),
                argumentSet("null key",
                        new TrustAnchorRefBuilder().withKey(null).withNewRef().withName("configmap").endRef().build(),
                        EMPTY_CONFIG_NMAP,
                        Condition.REASON_INVALID,
                        (ThrowingConsumer<String>) message -> assertThat(message).endsWith("must specify 'key'")),
                argumentSet("blank key",
                        new TrustAnchorRefBuilder().withKey("").withNewRef().withName("configmap").endRef().build(),
                        EMPTY_CONFIG_NMAP,
                        Condition.REASON_INVALID,
                        (ThrowingConsumer<String>) message -> assertThat(message).endsWith(".key should end with .pem, .p12 or .jks")),
                argumentSet("unsupported key file extension",
                        new TrustAnchorRefBuilder().withKey("/path/to/random.key").withNewRef().withName("configmap").endRef().build(),
                        EMPTY_CONFIG_NMAP,
                        Condition.REASON_INVALID,
                        (ThrowingConsumer<String>) message -> assertThat(message).endsWith(".key should end with .pem, .p12 or .jks")),
                argumentSet("Config map does not contain key",
                        new TrustAnchorRefBuilder().withKey("key.p12").withNewRef().withName("configmap").endRef().build(),
                        EMPTY_CONFIG_NMAP,
                        Condition.REASON_INVALID_REFERENCED_RESOURCE,
                        (ThrowingConsumer<String>) message -> assertThat(message).contains("referenced resource does not contain key")));
    }
}

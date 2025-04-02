/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;

import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.common.IngressRefBuilder;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.common.ProxyRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.findOnlyResourceNamed;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toByNameMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThatThrownBy(() -> stream.collect(toByNameMap()))
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

    static List<Arguments> maybeAddOrUpdateConditions() {
        var now = Clock.systemUTC().instant();
        Condition resolvedRefs = new ConditionBuilder()
                .withObservedGeneration(1L)
                .withLastTransitionTime(now)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withReason("reason")
                .withMessage("message")
                .build();

        Condition accepted = new ConditionBuilder()
                .withObservedGeneration(1L)
                .withLastTransitionTime(now)
                .withType(Condition.Type.Accepted)
                .withStatus(Condition.Status.FALSE)
                .withReason("reason")
                .withMessage("message")
                .build();

        Condition resolvedRefsLaterTime = new ConditionBuilder(resolvedRefs)
                .withLastTransitionTime(now.plus(1, ChronoUnit.MINUTES))
                .build();

        Condition resolvedRefsGen2 = new ConditionBuilder(resolvedRefs)
                .withObservedGeneration(2L)
                .build();

        Condition resolvedRefsGen2AndLaterTime = new ConditionBuilder(resolvedRefs)
                .withLastTransitionTime(now.plus(1, ChronoUnit.MINUTES))
                .withObservedGeneration(2L)
                .build();

        return List.of(
                Arguments.argumentSet("should add to empty list",
                        List.of(), resolvedRefs, List.of(resolvedRefs)),
                Arguments.argumentSet("add is idempotent",
                        List.of(resolvedRefs), resolvedRefs, List.of(resolvedRefs)),
                Arguments.argumentSet("returns totally ordered 1",
                        List.of(accepted), resolvedRefs, List.of(resolvedRefs, accepted)),
                Arguments.argumentSet("returns totally ordered 2",
                        List.of(resolvedRefs, accepted), resolvedRefs, List.of(resolvedRefs, accepted)),
                Arguments.argumentSet("returns totally ordered 3",
                        List.of(accepted, resolvedRefs), resolvedRefs, List.of(resolvedRefs, accepted)),
                Arguments.argumentSet("prefer arg when same observedGeneration 1",
                        List.of(resolvedRefs), resolvedRefsLaterTime, List.of(resolvedRefsLaterTime)),
                Arguments.argumentSet("prefer arg when same observedGeneration 2",
                        List.of(resolvedRefsLaterTime), resolvedRefs, List.of(resolvedRefs)),
                Arguments.argumentSet("prefer arg when same observedGeneration 3",
                        List.of(resolvedRefsGen2AndLaterTime), resolvedRefsGen2, List.of(resolvedRefsGen2)),
                Arguments.argumentSet("prefer arg when same observedGeneration 4",
                        List.of(resolvedRefsGen2), resolvedRefsGen2AndLaterTime, List.of(resolvedRefsGen2AndLaterTime)),
                Arguments.argumentSet("largest observedGeneration wins 1",
                        List.of(resolvedRefs), resolvedRefsGen2, List.of(resolvedRefsGen2)),
                Arguments.argumentSet("largest observedGeneration wins 2",
                        List.of(resolvedRefsLaterTime), resolvedRefsGen2, List.of(resolvedRefsGen2)),
                Arguments.argumentSet("replaces _all_ conditions with same type",
                        List.of(resolvedRefsLaterTime, resolvedRefs), resolvedRefsGen2, List.of(resolvedRefsGen2)),
                Arguments.argumentSet("existing condition with later generation not replaced 1",
                        List.of(resolvedRefs, resolvedRefsGen2), resolvedRefsLaterTime, List.of(resolvedRefsGen2)),
                Arguments.argumentSet("existing condition with later generation not replaced 2",
                        List.of(resolvedRefsGen2, resolvedRefs), resolvedRefsLaterTime, List.of(resolvedRefsGen2)));
    }

    @ParameterizedTest
    @MethodSource
    void maybeAddOrUpdateConditions(List<Condition> list, Condition condition, List<Condition> expectedResult) {
        assertThat(Conditions.maybeAddOrUpdateConditions(list, List.of(condition))).isEqualTo(expectedResult);
    }

}

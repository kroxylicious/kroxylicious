/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;

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

}

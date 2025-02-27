/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyspec.ClustersBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class ResourcesUtilTest {

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

}
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.proxy.api.v1alpha1.proxyspec.ClustersBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class ResourcesUtilTest {

    @Test
    void distinctClustersShouldFilterDupes() {
        var foo = new ClustersBuilder().withName("foo").build();
        var bar = new ClustersBuilder().withName("bar").build();
        var foo2 = new ClustersBuilder(foo).build();
        assertThat(ResourcesUtil.distinctClusters(List.of(foo, bar, foo2))).isEqualTo(List.of(foo, bar));
    }

}
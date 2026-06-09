/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.cluster;

import java.util.OptionalInt;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

class ProxyClusterListenerTest {

    private final ProxyClusterListener listener = new ProxyClusterListener() {
    };

    @Test
    void defaultOnMembershipChangedIsNoOp() {
        assertThatCode(() -> listener.onMembershipChanged(Set.of()))
                .doesNotThrowAnyException();
    }

    @Test
    void defaultOnVirtualClusterLeaderChangedIsNoOp() {
        assertThatCode(() -> listener.onVirtualClusterLeaderChanged("vc1", OptionalInt.of(1)))
                .doesNotThrowAnyException();
    }

    @Test
    void defaultOnPartitionAssignmentChangedIsNoOp() {
        assertThatCode(() -> listener.onPartitionAssignmentChanged("vc1", "__consumer_offsets", null))
                .doesNotThrowAnyException();
    }
}

/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.config.CipherSpec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CipherSpecResolverTest {
    @Test
    void fromPersistentIdShouldThrowIfUnknownPersistentId() {
        assertThatThrownBy(() -> CipherSpecResolver.ALL.fromSerializedId((byte) 123)).isExactlyInstanceOf(UnknownCipherSpecException.class);
    }

    @Test
    void persistentIdsShouldBeUnique() {
        assertThat(Arrays.stream(CipherSpec.values()).map(CipherSpecResolver.ALL::fromName).collect(Collectors.toSet()))
                                                                                                                        .hasSize(CipherSpec.values().length);
    }
}

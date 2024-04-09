/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.common.EncryptionException;
import io.kroxylicious.filter.encryption.crypto.WrapperV1;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WrapperVersionTest {

    @Test
    void unsupportedWrapperVersionThrowsOnUsage() {
        assertUnsupported(() -> WrapperV1.INSTANCE.writeWrapper(null, null, null, 1, null, null, null, null, null, null, null));
        assertUnsupported(() -> WrapperV1.INSTANCE.read(null, null, 1, null, null, null, null, null));
        assertUnsupported(() -> WrapperV1.INSTANCE.readSpecAndEdek(null, null, null));
    }

    private void assertUnsupported(ThrowableAssert.ThrowingCallable op) {
        assertThatThrownBy(op).isInstanceOf(EncryptionException.class).hasMessage("V1 wrappers are unsupported");
    }

}

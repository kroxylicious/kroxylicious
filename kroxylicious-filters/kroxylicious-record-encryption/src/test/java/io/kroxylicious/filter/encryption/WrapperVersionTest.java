/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;

import static io.kroxylicious.filter.encryption.WrapperVersion.V1_UNSUPPORTED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WrapperVersionTest {

    @Test
    void unsupportedWrapperVersionThrowsOnUsage() {
        assertUnsupported(() -> V1_UNSUPPORTED.writeWrapper(null, null, null, 1, null, null, null, null, null, null, null));
        assertUnsupported(() -> V1_UNSUPPORTED.read(null, null, 1, null, null, null, null, null));
        assertUnsupported(() -> V1_UNSUPPORTED.readSpecAndEdek(null, null, null));
    }

    private void assertUnsupported(ThrowableAssert.ThrowingCallable op) {
        assertThatThrownBy(op).isInstanceOf(EncryptionException.class).hasMessage("V1 wrappers are unsupported");
    }

}

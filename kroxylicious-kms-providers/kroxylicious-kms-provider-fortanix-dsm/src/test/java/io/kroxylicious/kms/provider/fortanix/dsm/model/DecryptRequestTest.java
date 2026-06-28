/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DecryptRequestTest {

    private static final SecurityObjectDescriptor KEY = new SecurityObjectDescriptor("kid", null, null);
    private static final byte[] IV = "iv".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CIPHERTEXT = "cipher".getBytes(StandardCharsets.UTF_8);

    @Test
    void rejectsEmptyCiphertext() {
        assertThatThrownBy(() -> new DecryptRequest(KEY, "alg", "mode", new byte[]{}, IV))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsEmptyIv() {
        assertThatThrownBy(() -> new DecryptRequest(KEY, "alg", "mode", CIPHERTEXT, new byte[]{}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStringMasksCiphertext() {
        assertThat(new DecryptRequest(KEY, "alg", "mode", IV, CIPHERTEXT))
                .hasToString(
                        "DecryptRequest{key=SecurityObjectDescriptor[kid=kid, name=null, transientKey=null], alg='alg', mode='mode', iv='*********', cipher='*********'}");
    }

}

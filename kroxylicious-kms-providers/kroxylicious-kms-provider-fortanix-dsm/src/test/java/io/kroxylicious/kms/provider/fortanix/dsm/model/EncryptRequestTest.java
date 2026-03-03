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

class EncryptRequestTest {

    private static final SecurityObjectDescriptor KEY = new SecurityObjectDescriptor("kid", null, null);

    @Test
    void rejectsEmptyPlain() {
        assertThatThrownBy(() -> new EncryptRequest(KEY, "alg", "mode", new byte[]{}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStringMasksPlaintext() {
        var plain = "mykey".getBytes(StandardCharsets.UTF_8);
        assertThat(new EncryptRequest(KEY, "alg", "mode", plain))
                .hasToString("EncryptRequest{key=SecurityObjectDescriptor[kid=kid, name=null, transientKey=null], alg='alg', mode='mode', plain='*********'}");
    }

}

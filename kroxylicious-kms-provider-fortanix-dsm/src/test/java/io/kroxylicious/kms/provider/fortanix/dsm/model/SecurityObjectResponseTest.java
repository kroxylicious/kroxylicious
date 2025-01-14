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

class SecurityObjectResponseTest {

    @Test
    void securityObjectIdentifiedByKid() {
        assertThat(new SecurityObjectResponse("kid", null, null))
                .extracting(SecurityObjectResponse::kid)
                .isEqualTo("kid");
    }

    @Test
    void securityObjectIdentifiedByTransientKey() {
        assertThat(new SecurityObjectResponse(null, "tk", null))
                .extracting(SecurityObjectResponse::transientKey)
                .isEqualTo("tk");
    }

    @Test
    void securityObjectWithTransientKeyAndExportedKeyMaterial() {
        var keyMaterial = "mykey".getBytes(StandardCharsets.UTF_8);
        assertThat(new SecurityObjectResponse(null, "tk", keyMaterial))
                .satisfies(sr -> {
                    assertThat(sr.transientKey()).isEqualTo("tk");
                    assertThat(sr.value()).isEqualTo(keyMaterial);
                });
    }

    @Test
    void toStringMasksExportedKeyMaterial() {
        var keyMaterial = "mykey".getBytes(StandardCharsets.UTF_8);
        assertThat(new SecurityObjectResponse(null, "tk", keyMaterial))
                .hasToString("SecurityObjectResponse{kid='null', transientKey='tk', value=*********}");
    }

    @Test
    void rejectsMissingIdentifier() {
        assertThatThrownBy(() -> new SecurityObjectResponse(null, null, new byte[]{}))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsMissingValue() {
        assertThatThrownBy(() -> new SecurityObjectResponse(null, "tk", new byte[]{}))
                .isInstanceOf(IllegalArgumentException.class);
    }

}

/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SecurityObjectDescriptorTest {
    @Test
    void noKeyRef() {
        assertThatThrownBy(() -> new SecurityObjectDescriptor(null, null, null))
                .isInstanceOf(NullPointerException.class);
    }
}
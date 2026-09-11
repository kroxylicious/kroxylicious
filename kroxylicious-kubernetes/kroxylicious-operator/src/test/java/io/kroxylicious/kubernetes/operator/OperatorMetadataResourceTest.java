/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OperatorMetadataResourceTest {

    @Test
    void shouldReturnOperatorVersionInfo() {
        var versionInfo = OperatorMain.versionInfo();

        assertThat(versionInfo.version()).isEqualTo("operator-metadata-version-test");
        assertThat(versionInfo.commitId()).isEqualTo("operator-metadata-commit-test");
    }
}

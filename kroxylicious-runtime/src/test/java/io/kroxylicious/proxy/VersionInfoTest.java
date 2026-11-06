/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VersionInfoTest {

    @Test
    void fromResourceShouldReadVersionAndCommitId() {
        // When
        VersionInfo versionInfo = VersionInfo.fromResource("io/kroxylicious/proxy/version-info-test.properties");

        // Then
        assertThat(versionInfo.version()).isEqualTo("1.2.3");
        assertThat(versionInfo.commitId()).isEqualTo("0123456789abcdef");
    }

    @Test
    void fromResourceShouldDefaultMissingPropertiesToUnknown() {
        // When
        VersionInfo versionInfo = VersionInfo.fromResource("io/kroxylicious/proxy/version-info-empty.properties");

        // Then
        assertThat(versionInfo.version()).isEqualTo("unknown");
        assertThat(versionInfo.commitId()).isEqualTo("unknown");
    }

    @Test
    void fromResourceShouldReturnUnknownWhenResourceIsMissing() {
        // When
        VersionInfo versionInfo = VersionInfo.fromResource("io/kroxylicious/proxy/does-not-exist.properties");

        // Then
        assertThat(versionInfo.version()).isEqualTo("unknown");
        assertThat(versionInfo.commitId()).isEqualTo("unknown");
    }
}

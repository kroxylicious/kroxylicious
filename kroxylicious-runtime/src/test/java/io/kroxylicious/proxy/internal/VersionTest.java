/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VersionTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "v1alpha1", "v1alpha2", "v1beta1", "v1beta2", "v1", "v2alpha1", "v2beta1", "v2",
            "v1alpha32767", "v32767alpha1", "v32767alpha32767", "v32767beta32767", "v32767" })
    void parseApiVersion(String s) {
        Version v = Version.parse(s);
        assertThat(v).hasToString(s);
        assertThat(v).isEqualByComparingTo(v);
        assertThat(v.isStable()).isEqualTo(!s.contains("alpha") && !s.contains("beta"));
        if (s.startsWith("v1") && s.length() > 2) {
            assertThat(v).isLessThan(Version.parse("v1"));
        }
        else if (s.startsWith("v2")) {
            assertThat(v).isGreaterThan(Version.parse("v1"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "v1Beta1", "v1gamma2", "v0", "v-1", "v1beta-1", "v1alpha0" })
    void parseRejectsIllegalApiVersion(String s) {
        assertThatThrownBy(() -> Version.parse(s)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwWhenUnstableApiIsNotExplicitlyAllowed() {
        Version unstableVersion = Version.parse("v1alpha1");
        assertThatThrownBy(() -> Version.throwUnlessApiIsAllowed("oh.look.an.UnstableApi", unstableVersion))
                .isInstanceOf(Version.DisallowedUnstableApiException.class)
                .hasMessageStartingWith("API 'oh.look.an.UnstableApi' has unstable version v1alpha1, which you have not opted into using. "
                        + "To opt-in to using this unstable API include 'oh.look.an.UnstableApi' in the comma-separated list of "
                        + "APIs given as the value of the KROXYLICIOUS_ALLOWED_UNSTABLE_APIS environment variable. "
                        + "For example 'KROXYLICIOUS_ALLOWED_UNSTABLE_APIS=oh.look.an.UnstableApi");
    }

    @Test
    void doNotThrowWhenStableApiIsNotExplicitlyAllowed() {
        Version stableVersion = Version.parse("v1");
        assertThatCode(() -> Version.throwUnlessApiIsAllowed("oh.look.an.UnstableApi", stableVersion))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @CsvSource({
            "v1, v1, true",
            "v1, v1beta1, false",
            "v1, v1alpha1, false",
            "v1beta1, v1beta1, true",
            "v1alpha1, v1alpha1, true",
            "v1beta2, v1beta1, false",
            "v1alpha2, v1alpha1, false",
            "v1, v2, false",
            "v2, v1, false",
            "v1beta1, v1alpha1, false",
            "v2alpha1, v1, false",
    })
    void isCompatibleWith(String running, String compiledAgainst, boolean expected) {
        Version runningVersion = Version.parse(running);
        Version compiledVersion = Version.parse(compiledAgainst);
        assertThat(runningVersion.isCompatibleWith(compiledVersion)).isEqualTo(expected);
    }

}
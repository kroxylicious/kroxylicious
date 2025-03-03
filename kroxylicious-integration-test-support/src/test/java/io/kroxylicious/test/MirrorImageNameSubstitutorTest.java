/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

class MirrorImageNameSubstitutorTest {

    @ParameterizedTest
    @CsvSource({ "x/y:1.0, mirror.gcr.io/x/y:1.0",
            "docker.io/x/y:1.0, mirror.gcr.io/x/y:1.0",
            "arbitrary.io/x/y:1.0, mirror.gcr.io/x/y:1.0",
            "quay.io/x/y:1.0, quay.io/x/y:1.0",
            "kroxylicious.quay.io/x/y:1.0, kroxylicious.quay.io/x/y:1.0",
            "ghcr.io/x/y:1.0, ghcr.io/x/y:1.0",
            "thing.ghcr.io/x/y:1.0, thing.ghcr.io/x/y:1.0",
            "gcr.io/x/y:1.0, gcr.io/x/y:1.0",
            "thing.gcr.io/x/y:1.0, thing.gcr.io/x/y:1.0" })
    void substitutes(String input, String output) {
        DockerImageName inputImageName = DockerImageName.parse(input);
        DockerImageName substituted = new MirrorImageNameSubstitutor().apply(inputImageName);
        String substitutedCanonical = substituted.asCanonicalNameString();
        assertThat(substitutedCanonical).isEqualTo(output);
    }

}
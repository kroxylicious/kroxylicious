/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.aws;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AwsKmsTestKmsFacadeFactoryTest {

    @Test
    void shouldBuildFacade() {
        // Given
        var factory = new AwsKmsTestKmsFacadeFactory();

        // When
        var facade = factory.build();

        // Then
        assertThat(facade)
                .isInstanceOf(AwsKmsTestKmsFacade.class);
    }
}

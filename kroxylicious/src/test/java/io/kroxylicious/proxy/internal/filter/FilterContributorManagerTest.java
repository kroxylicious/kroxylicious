/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.FilterContributor;

import static org.assertj.core.api.Assertions.assertThat;

class FilterContributorManagerTest {

    private FilterContributorManager testFilterContributorManager;

    @BeforeEach
    void setUp() {
        final List<FilterContributor> testFilterContributors = List.of(new TestFilterContributor());
        testFilterContributorManager = new FilterContributorManager(testFilterContributors::iterator);
    }

    @Test
    void shouldReturnFalseIfConfigNotRequired() {
        // Given

        // When
        final boolean actual = testFilterContributorManager.requiresConfig(TestFilterContributor.NO_CONFIG_REQUIRED_TYPE_NAME);

        // Then
        assertThat(actual).isFalse();
    }

    @Test
    void shouldReturnTrueIfConfigRequired() {
        // Given

        // When
        final boolean actual = testFilterContributorManager.requiresConfig(TestFilterContributor.CONFIG_REQUIRED_TYPE_NAME);

        // Then
        assertThat(actual).isTrue();
    }

}
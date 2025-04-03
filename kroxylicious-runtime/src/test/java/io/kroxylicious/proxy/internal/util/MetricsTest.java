/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Tag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetricsTest {

    @Test
    void shouldBuildTagList() {
        // Given

        // When
        List<Tag> tags = Metrics.tags();

        // Then
        assertThat(tags).isEmpty();
    }

    @Test
    void shouldBuildTagListWithValue() {
        // Given

        // When
        List<Tag> tags = Metrics.tags("TagA", "value1");

        // Then
        assertThat(tags).containsExactly(Tag.of("TagA", "value1"));
    }

    @Test
    void shouldBuildTagListWithTwoTags() {
        // Given

        // When
        List<Tag> tags = Metrics.tags("TagA", "value1",
                "TagB", "value2");

        // Then
        assertThat(tags).containsExactly(
                Tag.of("TagA", "value1"),
                Tag.of("TagB", "value2"));
    }

    @Test
    void shouldBuildTagListWithMultipleTags() {
        // Given

        // When
        List<Tag> tags = Metrics.tags("TagA", "value1",
                "TagB", "value2",
                "TagC", "value3");

        // Then
        assertThat(tags).containsExactly(
                Tag.of("TagA", "value1"),
                Tag.of("TagB", "value2"),
                Tag.of("TagC", "value3"));
    }

    @Test
    void shouldThrowIfTagNameHasNoValue() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> Metrics.tags("TagA"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowIfOneOfTwoTagNameHasNoValue() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> Metrics.tags("TagA", "value1", "TagB"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowIfOneOfSeveralTagsNameHasNoValue() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> Metrics.tags("TagA", "value1", "TagB", "value2", "TagC", "value3", "TagD"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowIfTagNameHasEmptyValue() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> Metrics.tags("TagA", " "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowIfOneOfTwoTagNameHasEmptyValue() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> Metrics.tags("TagA", "value1", "TagB", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowIfOneOfSeveralTagsNameHasEmptyValue() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> Metrics.tags("TagA", "value1", "TagB", "value2", "TagC", "value3", "TagD", "   "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
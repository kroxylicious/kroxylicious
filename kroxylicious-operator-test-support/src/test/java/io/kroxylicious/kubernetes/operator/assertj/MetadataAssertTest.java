/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;

class MetadataAssertTest {

    public static final String ANNOTATION_A = "AnnotationA";
    public static final String VALUE_1 = "Value1";

    @Test
    void shouldFailWithNoAnnotations() {
        // Given
        var thingWithMetadata = new KafkaProxyIngressBuilder().withNewMetadata().withName("thing").endMetadata().build();

        // When
        // Then
        Assertions.assertThatThrownBy(() -> Assertions.assertThatThrownBy(() -> MetadataAssert.assertThat(thingWithMetadata))).isInstanceOf(AssertionError.class);
    }

    @Test
    void shouldPassWithAnnotations() {
        // Given
        var thingWithMetadata = new KafkaProxyIngressBuilder().withNewMetadata().withName("thing").withAnnotations(Map.of(ANNOTATION_A, VALUE_1)).endMetadata().build();

        // When
        // Then
        MetadataAssert.assertThat(thingWithMetadata).hasAnnotations();
    }

    @Test
    void shouldFailWithEmptyAnnotations() {
        // Given
        var thingWithMetadata = new KafkaProxyIngressBuilder().withNewMetadata().withName("thing").withAnnotations(Map.of()).endMetadata().build();

        // When
        // Then
        Assertions.assertThatThrownBy(() -> MetadataAssert.assertThat(thingWithMetadata).hasAnnotations()).isInstanceOf(AssertionError.class);
    }

    @Test
    void shouldFailWithNoMetadata() {
        // Given
        var thingWithoutMetadata = new KafkaProxyIngressBuilder().withMetadata(null).build();

        // When
        // Then
        Assertions.assertThatThrownBy(() -> MetadataAssert.assertThat(thingWithoutMetadata).assertHasObjectMeta()).isInstanceOf(AssertionError.class);
    }

    @Test
    void shouldFailWhenAnnotationIsMissing() {
        // Given
        var thingWithoutMetadata = new KafkaProxyIngressBuilder().withNewMetadata().withName("thing").withAnnotations(Map.of(ANNOTATION_A, VALUE_1)).endMetadata()
                .build();

        // When
        // Then
        Assertions.assertThatThrownBy(
                () -> MetadataAssert.assertThat(thingWithoutMetadata)
                        .hasAnnotationSatisfying("MissingAnnotation", value -> Assertions.assertThat(value).isNotNull()))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void shouldFailWhenAnnotationHasWrongValue() {
        // Given
        var thingWithoutMetadata = new KafkaProxyIngressBuilder().withNewMetadata().withName("thing").withAnnotations(Map.of(ANNOTATION_A, VALUE_1)).endMetadata()
                .build();

        // When
        // Then
        Assertions.assertThatThrownBy(
                () -> MetadataAssert.assertThat(thingWithoutMetadata)
                        .hasAnnotationSatisfying(ANNOTATION_A, value -> Assertions.assertThat(value).isBlank()))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void shouldPassWhenAnnotationSatisfiesCondition() {
        // Given
        var thingWithoutMetadata = new KafkaProxyIngressBuilder().withNewMetadata().withName("thing").withAnnotations(Map.of(ANNOTATION_A, VALUE_1)).endMetadata()
                .build();

        // When
        // Then
        MetadataAssert.assertThat(thingWithoutMetadata)
                .hasAnnotationSatisfying(ANNOTATION_A, value -> Assertions.assertThat(value).isNotNull());
    }
}
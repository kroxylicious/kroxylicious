/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import java.util.function.Consumer;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.MapAssert;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * Assertions on the metadata of a Kubernetes resource.
 *
 * @param <T> the resource type
 */
@SuppressWarnings("UnusedReturnValue")
public class MetadataAssert<T extends HasMetadata> extends AbstractObjectAssert<MetadataAssert<T>, T> {
    private MetadataAssert(T actual) {
        super(actual, MetadataAssert.class);
    }

    /**
     * Creates an assertion on the metadata of the given resource.
     *
     * @param <T> the resource type
     * @param actual the resource to assert on
     * @return a new assertion
     */
    public static <T extends HasMetadata> MetadataAssert<T> assertThat(T actual) {
        return new MetadataAssert<>(actual);
    }

    /**
     * Asserts that the resource has an annotation with the given name whose value satisfies the
     * given requirements.
     *
     * @param annotationName the annotation name
     * @param expectedValueConsumer the requirements the annotation value must satisfy
     * @return a map assertion on the resource's annotations
     */
    public MapAssert<String, String> hasAnnotationSatisfying(String annotationName, Consumer<String> expectedValueConsumer) {
        return assertHasObjectMeta().hasAnnotationSatisfying(annotationName, expectedValueConsumer);
    }

    /**
     * Asserts that the resource has at least one annotation.
     *
     * @return a map assertion on the resource's annotations
     */
    public MapAssert<String, String> hasAnnotations() {
        return assertHasObjectMeta().hasAnnotations();
    }

    /**
     * Asserts that the resource has no annotations.
     */
    public void hasNoAnnotations() {
        assertHasObjectMeta().hasNoAnnotations();
    }

    /**
     * Asserts that the resource has non-null metadata and returns an assertion on it.
     *
     * @return an assertion on the resource's metadata
     */
    public ObjectMetaAssert assertHasObjectMeta() {
        assertThat(actual).isNotNull();
        return ObjectMetaAssert.assertThat(actual.getMetadata()).isNotNull();
    }

    /**
     * Asserts that the resource does not have an annotation with the given name.
     *
     * @param annotationName the annotation name
     * @return a map assertion on the resource's annotations
     */
    public MapAssert<String, String> doesNotHaveAnnotation(String annotationName) {
        return assertHasObjectMeta().doesNotHaveAnnotation(annotationName);
    }
}

/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import java.util.function.Consumer;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.MapAssert;

import io.fabric8.kubernetes.api.model.ObjectMeta;

/**
 * Assertions on an {@link ObjectMeta}.
 */
@SuppressWarnings("UnusedReturnValue")
public class ObjectMetaAssert extends AbstractObjectAssert<ObjectMetaAssert, ObjectMeta> {
    private ObjectMetaAssert(ObjectMeta actual) {
        super(actual, ObjectMetaAssert.class);
    }

    /**
     * Creates an assertion on the given metadata.
     *
     * @param actual the metadata to assert on
     * @return a new assertion
     */
    public static ObjectMetaAssert assertThat(ObjectMeta actual) {
        return new ObjectMetaAssert(actual);
    }

    /**
     * Asserts that the metadata has an annotation with the given name whose value satisfies the
     * given requirements.
     *
     * @param annotationName the annotation name
     * @param expectedValueConsumer the requirements the annotation value must satisfy
     * @return a map assertion on the annotations
     */
    public MapAssert<String, String> hasAnnotationSatisfying(String annotationName, Consumer<String> expectedValueConsumer) {
        return hasAnnotations()
                .hasEntrySatisfying(annotationName, expectedValueConsumer);
    }

    /**
     * Asserts that the metadata has at least one annotation.
     *
     * @return a map assertion on the annotations
     */
    public MapAssert<String, String> hasAnnotations() {
        return getAnnotationsAssert().isNotEmpty();
    }

    /**
     * Asserts that the metadata has no annotations.
     */
    public void hasNoAnnotations() {
        getAnnotationsAssert().isEmpty();
    }

    private MapAssert<String, String> getAnnotationsAssert() {
        return assertThat(actual)
                .isNotNull()
                .extracting(ObjectMeta::getAnnotations)
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class));
    }

    /**
     * Asserts that the metadata does not have an annotation with the given name.
     *
     * @param annotationName the annotation name
     * @return a map assertion on the annotations
     */
    public MapAssert<String, String> doesNotHaveAnnotation(String annotationName) {
        return getAnnotationsAssert().doesNotContainKey(annotationName);
    }
}
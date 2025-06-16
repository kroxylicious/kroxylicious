/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import java.util.Map;
import java.util.function.Consumer;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.MapAssert;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;

@SuppressWarnings("UnusedReturnValue")
public class ObjectMetaAssert extends AbstractObjectAssert<ObjectMetaAssert, ObjectMeta> {
    private ObjectMetaAssert(ObjectMeta actual) {
        super(actual, ObjectMetaAssert.class);
    }

    public static ObjectMetaAssert assertThat(ObjectMeta actual) {
        return new ObjectMetaAssert(actual);
    }

    public MapAssert<String, String> hasAnnotationSatisfying(String annotationName, Consumer<String> expectedValueConsumer) {
        return hasAnnotations()
                .hasEntrySatisfying(annotationName, expectedValueConsumer);
    }

    public MapAssert<String, String> hasAnnotations() {
        return getAnnotationsAssert().isNotEmpty();
    }

    public void hasNoAnnotations() {
        getAnnotationsAssert().isEmpty();
    }

    private MapAssert<String, String> getAnnotationsAssert() {
        return assertThat(actual)
                .isNotNull()
                .extracting(ObjectMeta::getAnnotations)
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class));
    }

    public MapAssert<String, String> doesNotHaveAnnotation(String annotationName) {
        return getAnnotationsAssert().doesNotContainKey(annotationName);
    }

    public ObjectMetaAssert hasName(String expected) {
        Assertions.assertThat(actual.getName()).isEqualTo(expected);
        return this;
    }

    public ObjectMetaAssert hasNamespace(String expected) {
        Assertions.assertThat(actual.getNamespace()).isEqualTo(expected);
        return this;
    }

    public ObjectMetaAssert hasOwnerRefs(OwnerReference... ownerReferences) {
        Assertions.assertThat(actual.getOwnerReferences()).containsExactly(ownerReferences);
        return this;
    }

    public ObjectMetaAssert hasLabels(Map<String, String> labels) {
        Assertions.assertThat(actual.getLabels()).isEqualTo(labels);
        return this;
    }
}
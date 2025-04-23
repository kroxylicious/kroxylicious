/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import java.util.function.Consumer;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.MapAssert;

import io.fabric8.kubernetes.api.model.ObjectMeta;

@SuppressWarnings("UnusedReturnValue")
public class ObjectMetaAssert extends AbstractObjectAssert<ObjectMetaAssert, ObjectMeta> {
    private ObjectMetaAssert(ObjectMeta actual) {
        super(actual, ObjectMetaAssert.class);
    }

    public static ObjectMetaAssert assertThat(ObjectMeta actual) {
        return new ObjectMetaAssert(actual);
    }

    public MapAssert<String, String> hasAnnotationSatisfying(String annotationName, Consumer<String> expectedValueConsumer) {
        return assertHasAnnotations()
                .hasEntrySatisfying(annotationName, expectedValueConsumer);
    }

    public MapAssert<String, String> assertHasAnnotations() {
        return assertThat(actual)
                .isNotNull()
                .extracting(ObjectMeta::getAnnotations)
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class));
    }

}